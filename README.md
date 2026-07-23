# Auralis

**A reactive kernel for Rust — `Signal<T>` + `TaskScope`.**

*[中文版](README_zh-CN.md)*

[![CI](https://github.com/chh-itt/auralis/actions/workflows/ci.yml/badge.svg)](https://github.com/chh-itt/auralis/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)

Three crates, zero platform dependencies. `Signal<T>` and `Memo<T>` track
dependencies at runtime. `TaskScope` manages task lifecycle through ownership.

---

## Quick Start

```rust
use auralis_signal::{Signal, Memo, batch};
use auralis_task::TaskScope;

// ---- Signal ----
let count = Signal::new(0);
count.set(1);
assert_eq!(count.read(), 1);

// ---- Memo (auto-tracking computed value) ----
let a = Signal::new(2);
let b = Signal::new(3);
let sum = Memo::new(move || a.read() + b.read());
assert_eq!(sum.read(), 5);

// ---- Signal::update (in-place mutation) ----
let items = Signal::new(vec![1, 2]);
items.update(|v| v.push(3));

// ---- Batch (multiple sets, one notification) ----
batch(|| { x.set(1); x.set(2); });

// ---- TaskScope (structured concurrency) ----
let scope = TaskScope::new();
let c = count.clone();
scope.spawn(async move {
    loop { c.changed().await; println!("count → {}", c.read()); }
});
drop(scope); // cancels everything inside
```

---

## What It Is

Auralis is a reactive kernel, not a framework. It makes three explicit trade-offs
that keep the codebase under ~3,000 lines per crate：

**No reactive graph.** Each signal has a flat subscriber list and a monotonic
version number. No topological propagation, no Clean/Check/Dirty state machine.
The cost: two effects reading the same dirty memo might each trigger a
recomputation.

**No arena allocation.** `Rc<RefCell<>>` uniformly. No `Copy` signals, no arena
lifetimes. The cost: reference-counting overhead on every read and clone.

**No multi-threaded storage.** Single-threaded by design (`!Send + !Sync`).
For multi-threaded scenarios, spin up isolated executor instances per request
or per thread — channels bridge the gap. The cost: you manage the isolation
boundaries yourself.

What you get: `#![forbid(unsafe_code)]` everywhere, zero dependencies for
`auralis-signal`, and a codebase you can read in an afternoon. No effect system.
No scheduler vocabulary. Just `Signal::read()` auto-subscribes, `Signal::set()`
auto-notifies. Tasks own their lifecycle through `TaskScope`.

---

## Crates

| Crate | Role | Size | Dependencies |
|---|---|---|---|
| `auralis-signal` | `Signal<T>`, `Memo<T>`, `SignalMap`, batch updates, change-detection futures | ~2,700 lines | **zero** |
| `auralis-task` | `TaskScope`, priority executor, `timer::sleep`, cancellation, context DI | ~3,000 lines | `auralis-signal` |
| `auralis-devtools` | `ReactiveSnapshot`, `diff_snapshots()`, `ChangeStream`, CLI | ~1,100 lines | `auralis-signal`, `auralis-task` |

---

## Used By

**[Burin](https://github.com/chh-itt/burin)** — a retained-mode Rust GUI
framework with 60 built-in widgets, dual GPU/CPU rendering backend, incremental
Taffy layout, and full-frame headless testing. Auralis powers Burin's entire
reactive pipeline：`Signal::set()` → `register_dirty` → incremental layout →
subtree cache → paint → GPU.

---

## Key Properties

**Safety**
- `#![forbid(unsafe_code)]` in all crates
- Panic-safe `Memo` — old subscriptions survive a panicked recompute, recover on next `read()`
- Panic-safe `batch` — `BatchGuard` RAII restores state on unwind
- Panic-safe cleanup — `CallbackHandle::drop` is `catch_unwind`-isolated
- Memo cycle detection via thread-local depth guard
- Iterative scope cancellation (BFS leaf-to-root), no stack overflow at 200+ levels

**Performance**
- Zero-dependency signal crate; single-threaded by design (`!Send` / `!Sync`)
- Configurable time budget — `set_global_time_budget(ms)`
- Proactive waker deregistration — no stale-waker accumulation
- `Signal::update()` — in-place mutation without cloning

**Diagnostics**
- `ReactiveSnapshot` — dump every signal, memo, and dependency edge as JSON
- `diff_snapshots()` — see exactly what changed between two frames
- `DerivationNode` tree — data-flow graph in React DevTools style
- `ChangeStream` — real-time change events via observer hooks
- CLI — `auralis-devtools dump | stream | serve`
- Labels on `Signal`, `Memo`, `TaskScope` for readable output
- `set_panic_hook()` to observe task failures
- Schedule observers — passive hooks on every signal mutation

**Ergonomics**
- `spawn()` returns `JoinHandle` — cancel or check individual tasks
- `watch` / `watch_effect` — auto-tracking side effects
- `Memo<T>` — lazy computed value with automatic dependency tracking
- `SignalMap<T,U,F>` — lightweight read-only projection

---

## Multi-threading

`Signal<T>` and `TaskScope` are `!Send + !Sync` by design — they live on the
executor thread. For cross-thread communication, bridge via a standard channel：

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
`Executor::new_instance()` + `TaskScope`. Enable `ssr-tokio` for per-task scope
storage. See `crates/auralis-task/examples/multi_thread_bridge.rs` for more
patterns.

---

## Feature Flags

| Feature | Crate | Enables |
|---------|-------|---------|
| `debug` | `auralis-task` | `dump_reactive_graph()` — signals, memos, and tasks in one snapshot |
| `diagnostics` | `auralis-signal` | Reactive node registry, `ReactiveNodeSnapshot`, `dump_registry()` |
| `ssr-tokio` | `auralis-task` | Tokio task-local storage for multi-request SSR |
| `ws-transport` | `auralis-devtools` | WebSocket server (`serve` command) via `tungstenite` |

---

## Running

```bash
cargo test --all
cargo test -p auralis-signal
cargo test -p auralis-task
cargo test -p auralis-devtools

cargo clippy --all-targets --all-features

cargo run --example signal_bench --release -p auralis-signal
cargo run --example scope_bench --release -p auralis-task

cargo run -p auralis-devtools -- dump
cargo doc --open
```

---

## Minimum Supported Rust Version

Rust 1.80+

---

## License

Licensed under either of

- MIT License ([LICENSE](LICENSE) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

at your option.
