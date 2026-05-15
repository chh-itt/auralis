# Architecture & Module Design

## Overview

```
auralis/
├── crates/
│   ├── auralis-signal/    # Signal<T>, Memo<T>, batch(), change-detection futures
│   ├── auralis-task/      # TaskScope tree, priority executor, cancellation, context DI
│   └── auralis-devtools/  # Diagnostic DevTools: JSON snapshots, change streams, CLI
├── tests/                 # Cross-crate integration tests
├── examples/              # Runnable examples
└── docs/                  # Design documentation
```

## auralis-signal: Reactive Primitives

### Modules

| File | Responsibility |
|------|---------------|
| `signal.rs` | `Signal<T>` core: value storage, version tracking, subscriber management, deferred notification state machine |
| `memo.rs` | `Memo<T>`: lazy auto-tracking computed signal, observer guard, panic-safe recompute |
| `batch.rs` | `BatchGuard`, `batch()`, `in_batch()`: batched signal updates with panic safety |
| `observer.rs` | `ObserverState`, `OBSERVER` thread-local: dependency-tracking infrastructure used by `Memo` |
| `future.rs` | `SignalChangedFuture`, `MapChangedFuture`, `FilterChangedFuture`: proactive waker deregistration |
| `registry.rs` | Thread-local reactive node registry for `dump_reactive_graph()` (behind `diagnostics` feature) |
| `lib.rs` | Crate root, public API re-exports |

### Notification State Machine

```
                 set() / bump_version()
                       │
                       ▼
              ┌─────────────────┐
              │ prepare_notify  │
              │ check flags     │
              └───────┬─────────┘
                      │
          notifying?  │  dirty?
          ┌───────────┤  ┌──────────┐
          ▼           │  ▼          │
      dirty=true      │ return      │ subscribers
      return None      │ None        │ empty → None
                      │             │
                      ▼             ▼
              ┌──────────────────────┐
              │  dirty=true          │
              │  snapshot subscribers│
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │ schedule_notification│
              │ (batch-aware)        │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  notification fires  │
              │  notifying=true      │
              │  dirty=false         │
              │  call subscribers    │
              │  ┌─────────────────┐ │
              │  │ re-entrant set? │ │
              │  │ → dirty=true    │ │
              │  └─────────────────┘ │
              │  notifying=false     │
              │  check dirty         │
              └──────────┬───────────┘
                         │
                  dirty? │
                  ┌──────┴──────┐
                  ▼             ▼
          schedule follow-up   done
          (notify_signal_state)
```

### Memo Panic Recovery

```
recompute():
  computing = true
  keep OLD subscriptions alive
  run_compute → collect NEW subscriptions in temp list
  ┌─ success → drain old, swap in new, dirty=false
  └─ panic   → drain partial new (clean up), old stays intact
  computing = false
```

Old subscriptions survive a panicked recompute, so the memo remains connected
to its sources and recovers on the next successful `read()`.

## auralis-task: Async Task Runtime

### Modules

| File | Responsibility |
|------|---------------|
| `executor.rs` | Single-threaded executor: dual priority queues, timer queue, time budget, deferred ops/callbacks, slot-based waker routing, instance isolation, configurable panic hook |
| `scope.rs` | `TaskScope` tree: parent/child relations, iterative cancellation, suspend/resume, context DI, `CallbackHandle` |
| `timer.rs` | `timer::sleep()` cooperative delay, `SleepFuture` with deadline re-check |
| `debug.rs` | `dump_reactive_graph()` — unified reactive graph snapshot including signals, memos, and tasks (behind `debug` feature) |
| `lib.rs` | Crate root, public API + `Priority` enum |

### Executor Architecture

```
flush() → Executor::flush_instance(&global_executor)
            ├─ Step 0: drain expired timers (all if no TimeSource)
            ├─ Step 1: execute deferred ops (set_deferred, etc.)
            ├─ Step 2: drain deferred signal callbacks (time-budgeted, catch_unwind isolated)
            └─ Step 3: main poll loop
                ├─ High-priority queue first
                ├─ Temporarily remove future (avoids borrow conflicts)
                ├─ Inject scope, inject task id (for timer::sleep), catch_unwind, poll
                └─ Time budget exceeded → schedule continuation
            └─ drain PENDING_WAKES (buffered wake-ups during RefCell borrow)
```

**Key design points:**

- **"Take out before poll" pattern:** the future is temporarily removed from
  the task table before polling, so nested spawns/wakes never hit a borrowed
  `RefCell`.
- **TaskWaker:** stores `(task_id, priority, slot_id, generation)`.  The
  slot_id indexes into a thread-local `Vec<Slot>` table; the generation
  counter invalidates stale wakers after executor destruction.  This keeps
  the waker `Send + Sync` without holding an `Rc`.
- **Timer queue:** `BTreeMap<deadline_ms, Vec<TaskId>>`, checked at Step 0.
  Each `TaskState` has a `timer_deadline` reverse index for O(1) cleanup on
  task cancellation.  Without a `TimeSource`, all timers expire on every flush.
- **Time budget:** configurable (default 8 ms) via `set_global_time_budget`
  or `Executor::set_time_budget`. Set to `u64::MAX` to disable.
- **Instance executor:** `Executor::new_instance()` creates a fully
  isolated executor (e.g. per SSR request).

**Signal routing constraint:**

The global signal schedule hook (installed by `init_flush_scheduler`) and
functions like `schedule_callback` / `set_deferred` now use
`current_executor_instance()`, which checks for an active `with_executor`
context first and falls back to the global executor.  For multi-instance
users this means:

1. `init_flush_scheduler` must be called at least once (otherwise `set`
   falls back to synchronous execution).
2. `with_executor` sets the current executor for signal callbacks and
   `set_deferred` calls within its scope.

For single-threaded use (Wasm, game loop, CLI), no special care is needed:
call `init_flush_scheduler` once at startup and never use `with_executor`.

### TaskScope Tree

Each scope holds an `Rc<RefCell<Executor>>` strong reference, so the executor
lives at least as long as the scope — essential for safe cancellation during drop.

```
TaskScope::with_executor(&ex)    # explicit executor, primary API
TaskScope::new()                 # convenience: delegates to global executor
├─ scope_1.spawn(future_a)       # routes through scope_1's executor
├─ child = TaskScope::new_child(&scope_1)
│   └─ inherits parent's executor
└─ drop(scope_1)
    ├─ CallbackHandle dropped first (disconnect signal chains)
    ├─ BFS collect scope_1 + child
    ├─ Each scope's cancel_scope_tasks_on(&scope.executor, id)
    └─ Leaf-to-root cancel all tasks on the correct executor
```

### Scope Suspend / Resume

- `suspend()` cascades: suspending a parent suspends all children
- Suspended tasks remain in the task table but are skipped during polling
- `resume()` cascades and re-enqueues all scope tasks

### Context System

Type-safe dependency injection that walks up the scope tree:

```rust
scope.provide(42i32);                  // store an i32
let val: Option<Rc<i32>> = scope.consume(); // look up, walking parent chain
let val: Rc<i32> = scope.expect_context();  // same, but panics if missing
```

Child scope values shadow parent scope values of the same type.

## Diagnostics Infrastructure

Auralis provides built-in introspection for debugging reactive systems:

### Labels

`Signal`, `Memo`, and `TaskScope` each carry an optional label (a short string)
that appears in `Debug` output and `dump_reactive_graph()`.  Labels are always
available — the overhead is a single `Rc<RefCell<Option<String>>>` per node.

```rust
let sig = Signal::new(0);
sig.set_label("counter");
assert_eq!(sig.label(), Some("counter".to_string()));
```

### Reactive Node Registry

Behind the `diagnostics` feature (enabled automatically by `auralis-task`'s
`debug` feature), every `Signal::new` and `Memo::new` registers a callback in
a thread-local registry.  The callback captures `Weak` references to the node's
internal state; dead nodes (dropped signals/memos) are automatically pruned.

`dump_registry()` returns a `Vec<ReactiveNodeSnapshot>` with the label, version,
subscriber count, dirty state (for memos), compute count, and dependency count
of every live node.

### Schedule Observers

`add_schedule_observer(Box<dyn Fn()>)` registers a passive hook that fires on
every signal mutation (`set`, `update`, `bump_version`).  Multiple observers
can coexist.  A generation-based `ObserverToken` prevents stale-token misuse.
Observers are individually `catch_unwind`-isolated and re-entrant calls are
silently skipped.

### `dump_reactive_graph()`

Unified diagnostic output (behind `auralis-task`'s `debug` feature) showing
all signals, memos, and tasks in one snapshot:

```text
=== Auralis Reactive Graph ===
Signals: 3  Memos: 2  Tasks: 4

── Signals ──
  "counter"  ver=42  subs=1  addr=0x...
  (unnamed)  ver=7   subs=0  addr=0x...

── Memos ──
  "sum"  ver=42  subs=1  dirty=false  computed=15x  deps=2  addr=0x...

── Tasks ──
Scope 1 "root":
  task 0  [L]  queued
```

`dump_task_tree()` remains available as a backward-compatible alias.

## auralis-devtools: Inspection & Diagnostics

### Modules

| File | Responsibility |
|------|---------------|
| `snapshot.rs` | `ReactiveSnapshot` struct, `snapshot()` — calls `dump_registry()` and formats a JSON-serializable snapshot including memo dependency addresses |
| `stream.rs` | `ChangeReceiver`, `change_stream()` — installs a schedule observer, delivers change events through an `mpsc` channel for efficient blocking via `wait_timeout()` |
| `main.rs` (bin) | CLI: `dump` (one-shot JSON to stdout), `stream` (change events to stdout), `serve` (WebSocket server, requires `ws-transport` feature) |

### Data Flow

```
Signal mutation → notify_schedule_observers()
  → change_stream observer increments seq + mpsc send
  → ChangeReceiver::wait_timeout() wakes up
  → caller calls snapshot() → dump_registry() → JSON
```

### CLI Usage

```bash
# One-shot snapshot
cargo run -p auralis-devtools -- dump

# Pipe-friendly change stream
cargo run -p auralis-devtools -- stream

# WebSocket server (requires ws-transport feature)
cargo run -p auralis-devtools --features ws-transport -- serve
```

## Feature Flags

| Feature | Crate | Enables |
|---------|-------|---------|
| `debug` | `auralis-task` | `dump_reactive_graph()` + reactive node registry (forwards to `auralis-signal/diagnostics`) |
| `diagnostics` | `auralis-signal` | Reactive node registry, `ReactiveNodeSnapshot`, `dump_registry()` |
| `ssr-tokio` | `auralis-task` | `init_scope_store_tokio()` for tokio task-local storage |
| `ws-transport` | `auralis-devtools` | WebSocket server for `auralis-devtools serve` (adds `tungstenite`) |

## Build Configuration

Release profile optimised for Wasm binary size:

```toml
[profile.release]
opt-level     = "s"
lto           = true
codegen-units = 1
strip         = true
```
