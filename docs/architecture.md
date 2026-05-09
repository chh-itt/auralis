# Architecture & Module Design

## Overview

```
auralis/
├── crates/
│   ├── auralis-signal/    # Signal<T>, Memo<T>, batch(), change-detection futures
│   └── auralis-task/      # TaskScope tree, priority executor, cancellation, context DI
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
| `debug.rs` | `dump_task_tree()` diagnostic (behind `debug` feature) |
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

```
TaskScope::new()
├─ scope_1.spawn(future_a)       # future_a belongs to scope_1
├─ child = TaskScope::new_child(&scope_1)
│   └─ child.spawn(future_b)     # future_b belongs to child
└─ drop(scope_1)
    ├─ CallbackHandle dropped first (disconnect signal chains)
    ├─ BFS collect scope_1 + child
    └─ Leaf-to-root cancel all tasks
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

## Feature Flags

| Feature | Enables |
|---------|---------|
| `debug` | `dump_task_tree()` diagnostic snapshot |
| `ssr-tokio` | `init_scope_store_tokio()` for tokio task-local storage |

## Build Configuration

Release profile optimised for Wasm binary size:

```toml
[profile.release]
opt-level     = "s"
lto           = true
codegen-units = 1
strip         = true
```
