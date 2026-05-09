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
| `executor.rs` | Single-threaded executor: dual priority queues, time budget, deferred ops/callbacks, instance isolation, configurable panic hook |
| `scope.rs` | `TaskScope` tree: parent/child relations, iterative cancellation, suspend/resume, context DI, `CallbackHandle` |
| `debug.rs` | `dump_task_tree()` diagnostic (behind `debug` feature) |
| `lib.rs` | Crate root, public API + `Priority` enum |

### Executor Architecture

```
flush() → Executor::flush_instance(&global_executor)
            ├─ Step 1: execute deferred ops (set_deferred, etc.)
            ├─ Step 2: drain deferred signal callbacks (time-budgeted)
            └─ Step 3: main poll loop
                ├─ High-priority queue first
                ├─ Temporarily remove future (avoids borrow conflicts)
                ├─ Inject scope, catch_unwind, poll
                └─ Time budget exceeded → schedule continuation
```

**Key design points:**

- **"Take out before poll" pattern:** the future is temporarily removed from
  the task table before polling, so nested spawns/wakes never hit a borrowed
  `RefCell`.
- **TaskWaker:** carries only `task_id: u64` and priority — trivially
  `Send + Sync` for `Waker::from`.
- **Time budget:** configurable (default 8 ms) via `set_global_time_budget`
  or `Executor::set_time_budget`. Set to `u64::MAX` to disable.
- **Instance executor:** `Executor::new_instance()` creates a fully
  isolated executor (e.g. per SSR request).

**Signal routing constraint:**

Signal notifications use a single global schedule hook (installed by the
first `init_flush_scheduler` call).  The hook routes callbacks to **the
executor that is current when the notification fires** — not the executor
that was current when `Signal::set` was called.  For multi-instance users
this means:

1. `init_flush_scheduler` must be called at least once (otherwise `set`
   falls back to synchronous execution).
2. `with_executor` must wrap the entire request lifecycle — from signal
   creation through the final flush — so that deferred callbacks land in
   the correct instance.

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
