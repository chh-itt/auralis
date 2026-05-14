# auralis-task

**Scoped async task runtime with cancellation and priority scheduling.**

[![CI](https://github.com/chh-itt/auralis/actions/workflows/ci.yml/badge.svg)](https://github.com/chh-itt/auralis/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/chh-itt/auralis/blob/main/LICENSE)
[![crates.io](https://img.shields.io/crates/v/auralis-task.svg)](https://crates.io/crates/auralis-task)

Built on `auralis-signal`. `#![forbid(unsafe_code)]`. Single-threaded by design.

## Overview

| Type | Role |
|------|------|
| `TaskScope` | Owns spawned tasks on an executor; `with_executor(&ex)` for explicit ownership, `new()` for global |
| `Executor` | Single-threaded async executor with high/low priority queues |
| `Priority` | `High` or `Low` — high dequeued first each flush cycle |
| `CallbackHandle` | RAII guard for signal subscriptions (dropped before tasks on scope cancel) |
| `JoinHandle` | Returned by `spawn()` — cancel or check completion of a single task |
| `TaskScope::on_cleanup(f)` | Register a cleanup closure (sugar for `CallbackHandle::new`) |
| `TaskScope::watch(sig, f)` | Run `f` on each signal change |
| `TaskScope::watch_effect(f)` | Auto-tracking effect — re-runs when any signal read inside `f` changes |
| `set_deferred(sig, val)` | Safe `Signal::set` from `Drop` contexts |
| `yield_now()` | Yield control back to the executor once |
| `schedule_callback(f)` | Run `f` at the start of the next flush |
| `timer::sleep(dur)` | Cooperative async delay (requires TimeSource for real-time; degrades to yield_now otherwise) |
| `TaskScope::is_cancelled()` | Check whether the scope has been dropped and its tasks cancelled |
| `TaskScope::set_label(l)` / `label()` | Optional human-readable label for diagnostics |
| `dump_reactive_graph()` | Unified snapshot of all signals, memos, and tasks (behind `debug` feature) |
| `dump_task_tree()` | Backward-compatible alias for `dump_reactive_graph()` |

## Quick Start

```rust
use auralis_signal::Signal;
use auralis_task::{TaskScope, init_flush_scheduler, set_global_time_budget};

// Set up the executor
init_flush_scheduler(std::rc::Rc::new(MyScheduleFlush));

// Structured concurrency
let scope = TaskScope::new();
let sig = Signal::new(0);
let s = sig.clone();
scope.spawn(async move {
    loop {
        let val = s.changed().await;
        println!("count → {val}");
    }
});
drop(scope); // cancels all spawned tasks
```

## Feature Flags

| Feature | Enables |
|---------|---------|
| `debug` | `dump_reactive_graph()` — signals, memos, and tasks diagnostic snapshot; enables `auralis-signal/diagnostics` |
| `ssr-tokio` | `init_scope_store_tokio()` for multi-request SSR isolation |

## Key Properties

- **Scope lifecycle** — `TaskScope::drop` only cancels on the last reference (`Rc::strong_count == 1`), matching `Memo::drop`. The `cancelled` flag is stored outside the `RefCell` so it is always settable, even during re-entrant drop.
- **Iterative scope cancellation** — BFS collect + leaf-to-root cancel, 200+ nesting levels without stack overflow. Cancel directly looks up tasks by id (no full-table scan).
- **Panic-safe cleanup** — `CallbackHandle::drop` is `catch_unwind`-isolated; a panicking cleanup closure does not corrupt scope teardown.
- **Configurable time budget** — `set_global_time_budget(ms)`, default 8 ms; set to `u64::MAX` to disable
- **Panic hook** — `set_panic_hook(hook)` to observe task failures with task_id and scope_id
- **Explicit executor ownership** — `TaskScope::with_executor(&ex)` stores an `Rc` strong reference; spawn, cancel, and resume all route through the scope's executor without thread-local lookup. `TaskScope::new()` delegates to the global executor as a convenience.
- **Instance isolation** — `Executor::new_instance()` for multi-threaded SSR; slot-based waker routing with generation counters
- **Cooperative timer** — `timer::sleep(dur)` works with any `TimeSource` implementation
- **Panic-safe callbacks** — each deferred signal callback is `catch_unwind`-isolated in the executor flush
- **Labels** — `TaskScope` supports optional labels via `set_label()`
- **Diagnostics** — `dump_reactive_graph()` (behind `debug` feature) shows all signals, memos, and tasks in one snapshot

## License

Licensed under either of [MIT](https://github.com/chh-itt/auralis/blob/main/LICENSE) or Apache 2.0 at your option.
