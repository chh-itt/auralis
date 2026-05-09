# Auralis

**An async-first reactive kernel for Rust: `Signal<T>` + `TaskScope`.**

*[中文版](README_zh-CN.md)*

[![CI](https://github.com/chh-itt/auralis/actions/workflows/ci.yml/badge.svg)](https://github.com/chh-itt/auralis/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)

Two crates, zero platform dependencies, one idea: **reactive = pausable
async tasks; lifecycle = ownership + structured concurrency.**

---

## Crates

| Crate | Role | Dependencies |
|---|---|---|
| `auralis-signal` | `Signal<T>`, `Memo<T>`, `SignalMap`, batch updates, change-detection futures | **zero** |
| `auralis-task` | `TaskScope`, priority executor, cancellation, context DI, panic hook | `auralis-signal` only |

## Quick Start

```rust
use auralis_signal::{Signal, Memo, batch};
use auralis_task::{TaskScope, set_global_time_budget};

// ---- Signal ----
let count = Signal::new(0);
count.set(1);
assert_eq!(count.read(), 1);

// ---- Memo (auto-tracking computed value) ----
let a = Signal::new(2);
let b = Signal::new(3);
let sum = Memo::new(move || a.read() + b.read());
assert_eq!(sum.read(), 5);

// ---- SignalMap (lightweight read-only projection) ----
let names = Signal::new(vec!["alice", "bob"]);
let len = names.map(|v: &Vec<&str>| v.len());
assert_eq!(len.read(), 2);

// ---- Batch (multiple sets, one notification) ----
let x = Signal::new(0);
batch(|| {
    x.set(1);
    x.set(2);
    x.set(3);
});
assert_eq!(x.read(), 3);

// ---- TaskScope (structured concurrency) ----
let scope = TaskScope::new();
let c = count.clone();
scope.spawn(async move {
    loop {
        let val = c.changed().await;
        println!("count → {val}");
    }
});
drop(scope); // cancels all spawned tasks
```

## Why

Reactive programming has historically meant learning a new runtime
vocabulary — effects, cleanups, derived-state graphs, scheduler ticks.
Auralis reduces it to things Rust programmers already know:

- **await a signal** — the task suspends until the value changes
- **scope owns tasks** — dropping the scope cancels everything inside
- **events / timers / fetch are futures** — compose with `select!`, `join!`

No `on_cleanup` hooks, no manual cancel tokens, no "effect system."
Just async Rust.

## Key Properties

- **`#![forbid(unsafe_code)]`** in both crates — zero unsafe
- **`#![warn(clippy::all, clippy::pedantic)]`** — strict linting
- **Zero-dependency** signal crate
- **Single-threaded by design** (`!Send` / `!Sync`); use `Executor::new_instance()` for multi-threaded isolation
- **Configurable time budget** — `set_global_time_budget(ms)` for different frame rates
- **Panic hook** — `set_panic_hook(hook)` to observe task failures
- **Panic-safe batch** — `BatchGuard` RAII restores state on unwind
- **Panic-safe Memo** — old subscriptions survive a panicked recompute
- **Proactive waker deregistration** — no stale-waker accumulation
- **Iterative scope cancellation** — BFS leaf-to-root, no stack overflow at 200+ levels

## Workspace Structure

```
crates/
  auralis-signal/       # Signal<T>, Memo<T>, SignalMap<T,U,F>, batch()
    src/
      signal.rs         # Signal state machine, subscriber management
      memo.rs           # Memo lazy tracking, panic-safe recompute
      batch.rs          # BatchGuard, batch(), in_batch()
      observer.rs       # ObserverState, OBSERVER thread-local
      future.rs         # SignalChangedFuture, MapChangedFuture, FilterChangedFuture
  auralis-task/         # TaskScope tree, executor, context DI
    src/
      executor.rs       # Priority executor, time budget, deferred callbacks
      scope.rs          # TaskScope, CallbackHandle, context system
      debug.rs          # dump_task_tree() (feature-gated)
    examples/
      counter.rs        # Runnable CLI demo
    tests/
      signal_task_integration.rs  # Cross-crate integration tests
docs/
  vision-and-design.md  # Design philosophy (EN)
  architecture.md       # Architecture & modules (EN)
  愿景与设计理念.md       # Design philosophy (ZH)
  架构与模块设计.md       # Architecture & modules (ZH)
```

## Feature Flags

| Feature | Crate | Enables |
|---------|-------|---------|
| `debug` | `auralis-task` | `dump_task_tree()` diagnostic |
| `ssr-tokio` | `auralis-task` | Tokio task-local storage for multi-request SSR |

## Running

```bash
# All tests
cargo test --all

# Specific crate
cargo test -p auralis-signal
cargo test -p auralis-task

# Linting
cargo clippy --all-targets -- -D warnings

# Benchmarks (host-side, non-Wasm)
cargo bench -p auralis-signal
cargo bench -p auralis-task

# Example
cargo run --example counter

# Multi-thread bridge example
cargo run --example multi_thread_bridge -p auralis-task

# Docs
cargo doc --open
```

## Minimum Supported Rust Version

Rust 1.80+

## License

Licensed under either of

- MIT License ([LICENSE](LICENSE) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

at your option.
