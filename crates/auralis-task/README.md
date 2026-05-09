# auralis-task

**Scoped async task runtime with cancellation and priority scheduling.**

[![CI](https://github.com/chh-itt/auralis/actions/workflows/ci.yml/badge.svg)](https://github.com/chh-itt/auralis/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/chh-itt/auralis/blob/main/LICENSE)
[![crates.io](https://img.shields.io/crates/v/auralis-task.svg)](https://crates.io/crates/auralis-task)

Built on `auralis-signal`. `#![forbid(unsafe_code)]`. Single-threaded by design.

## Overview

| Type | Role |
|------|------|
| `TaskScope` | Owns spawned tasks; dropping cancels everything inside (BFS leaf-to-root, no stack overflow) |
| `Executor` | Single-threaded async executor with high/low priority queues |
| `Priority` | `High` or `Low` — high dequeued first each flush cycle |
| `CallbackHandle` | RAII guard for signal subscriptions (dropped before tasks on scope cancel) |
| `set_deferred(sig, val)` | Safe `Signal::set` from `Drop` contexts |
| `yield_now()` | Yield control back to the executor once |
| `schedule_callback(f)` | Run `f` at the start of the next flush |

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
| `debug` | `dump_task_tree()` diagnostic snapshot |
| `ssr-tokio` | `init_scope_store_tokio()` for multi-request SSR isolation |

## Key Properties

- **Iterative scope cancellation** — BFS collect + leaf-to-root cancel, 200+ nesting levels without stack overflow
- **Configurable time budget** — `set_global_time_budget(ms)`, default 8 ms; set to `u64::MAX` to disable
- **Panic hook** — `set_panic_hook(hook)` to observe task failures with task_id and scope_id
- **Instance isolation** — `Executor::new_instance()` + `with_executor()` for multi-threaded SSR

## License

Licensed under either of [MIT](https://github.com/chh-itt/auralis/blob/main/LICENSE) or Apache 2.0 at your option.
