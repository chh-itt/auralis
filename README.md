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
| `auralis-signal` | `Signal<T>`, `Memo<T>`, `SignalMap`, `memo!` macro, batch updates, change-detection futures | **zero** |
| `auralis-task` | `TaskScope`, priority executor, `timer::sleep`, cancellation, context DI, panic hook | `auralis-signal` only |

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

// ---- Signal::update (in-place mutation, zero clone) ----
let items = Signal::new(vec![1, 2]);
items.update(|v| v.push(3));
assert_eq!(items.read(), vec![1, 2, 3]);

// ---- Signal::read_untracked (read without subscribing) ----
let config = Signal::new("dark_mode");
let _mode = config.read_untracked(); // won't trigger re-computation

// ---- TaskScope (structured concurrency) ----
let scope = TaskScope::new();
let c = count.clone();
let handle = scope.spawn(async move {
    loop {
        let val = c.changed().await;
        println!("count → {val}");
    }
});
handle.cancel(); // cancel a single task, or drop scope to cancel all
scope.on_cleanup(|| println!("scope dropped"));

// ---- watch_effect (auto-tracking side effect) ----
scope.watch_effect(|| {
    println!("sum = {}", sum.read());
});
drop(scope); // cancels all spawned tasks + runs cleanup
```

## Why

Reactive programming has historically meant learning a new runtime
vocabulary — effects, cleanups, derived-state graphs, scheduler ticks.
Auralis reduces it to things Rust programmers already know:

- **await a signal** — the task suspends until the value changes
- **scope owns tasks** — dropping the scope cancels everything inside
- **events / timers / fetch are futures** — compose with `select!`, `join!`

No manual cancel tokens, no "effect system." Just async Rust.

## Design bet

Auralis is **10× smaller, not 10× more powerful.** It sacrifices three things
that full reactive frameworks need:

- **No reactive graph.** Each signal has a flat subscriber list and a
  monotonic version number. No topological propagation, no Clean/Check/Dirty
  state machine. The tradeoff: two effects reading the same dirty memo
  might each trigger recomputation.
- **No arena allocation.** `Rc<RefCell<>>` uniformly. No `Copy` signals, no
  arena lifetimes. The tradeoff: reference-counting overhead on every read
  and clone.
- **No multi-threaded storage backend.** Single-threaded by design
  (`!Send + !Sync`). For multi-threaded SSR, spin up isolated executors
  per request.

What you get: ~1,400 lines of implementation across two crates, zero
dependencies for the signal layer, `#![forbid(unsafe_code)]`. The entire
reactive layer fits in your head after one coffee. If you need to debug
why an effect didn't fire, you step through a flat subscriber list, not
a graph.

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
- **Memo cycle detection** — thread-local depth guard catches circular dependencies
- **Iterative scope cancellation** — BFS leaf-to-root, no stack overflow at 200+ levels; direct task-id lookup (no full-table scan)
- **`Signal::update()`** — in-place mutation without cloning
- **`JoinHandle`** from `spawn()` — cancel or check individual tasks
- **`watch` / `watch_effect`** — auto-tracking side effects
- **Panic-safe cleanup** — `CallbackHandle::drop` is `catch_unwind`-isolated

## Multi-threading

`Signal<T>` and `TaskScope` are `!Send + !Sync` by design — they live on the
executor thread.  For cross-thread communication, bridge via a standard
channel: the worker thread owns the `Sender` (which *is* `Send`), and the
host thread drains the `Receiver` into `sig.set()`.

```rust
use std::sync::mpsc;
use std::thread;

let sig = Signal::new(0i32);
let (tx, rx) = mpsc::channel();

thread::spawn(move || { tx.send(42).unwrap(); });

for msg in rx { sig.set(msg); }
assert_eq!(sig.read(), 42);
```

For multi-request SSR isolation (Tokio), each request gets its own
`Executor::new_instance()` + `TaskScope`, wrapped via `with_executor`.
Enable the `ssr-tokio` feature for per-task scope storage.

See `crates/auralis-task/examples/multi_thread_bridge.rs` for more patterns
(multiple producers, oneshot results).

## Tokio Integration

Auralis is runtime-agnostic, but pairs naturally with Tokio for SSR.
**Tokio handles I/O** (network, DB, timers) and feeds results into signals.
**Auralis handles the reactive cascade** — signals → memos → effects →
rendered output.  The boundary is a single `signal.set()`.

```rust
use auralis_signal::Signal;
use auralis_task::{Executor, TaskScope, with_executor};

// Per-request: Tokio fetches data, Auralis renders.
let ex = Executor::new_instance();
let scope = TaskScope::with_executor(&ex);
let data = Signal::new(None::<Json>);

// Reactive effect: re-render on every data change.
scope.spawn(async move {
    loop { data.changed().await; render(data.read()); }
});

// Tokio I/O → signal → reactive cascade →
// Executor::flush_instance(&ex) → output ready.
let json = reqwest::get(url).await?.json().await?;
with_executor(&ex, || data.set(Some(json)));
Executor::flush_instance(&ex);

// Drop scope → all reactive state cleaned up. No manual tokens.
drop(scope);
```

For per-task scope storage with Tokio (required for `current_scope()`
inside spawned tasks), enable the `ssr-tokio` feature and call
`init_scope_store_tokio()` once at startup.

See `demos/tokio-ssr/` for a runnable demo with concurrent requests.

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
  auralis-task/         # TaskScope tree, executor, timer, context DI
    src/
      executor.rs       # Priority executor, time budget, deferred callbacks
      scope.rs          # TaskScope, CallbackHandle, context system
      timer.rs          # timer::sleep() cooperative delay
        debug.rs          # dump_task_tree() (feature-gated)
    examples/
      counter.rs        # Runnable CLI demo
    tests/
      signal_task_integration.rs  # Cross-crate integration tests
demos/
  egui-demo/            # Auralis vs plain egui comparison
    examples/
      perf_report.rs    # Headless performance benchmark
  wasm-counter/         # Wasm reactive counter (Signal + Memo + timer)
  cli-multitask/        # CLI multi-task with Ctrl+C cancellation
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
cargo run --example signal_bench --release -p auralis-signal
cargo run --example scope_bench --release -p auralis-task

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
