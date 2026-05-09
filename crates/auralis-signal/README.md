# auralis-signal

**Reactive signal primitive with version tracking and proactive waker deregistration.**

[![CI](https://github.com/chh-itt/auralis/actions/workflows/ci.yml/badge.svg)](https://github.com/chh-itt/auralis/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/chh-itt/auralis/blob/main/LICENSE)
[![crates.io](https://img.shields.io/crates/v/auralis-signal.svg)](https://crates.io/crates/auralis-signal)

Zero dependencies. `#![forbid(unsafe_code)]`. Single-threaded by design (`!Send` / `!Sync`).

## Overview

| Type | Role |
|------|------|
| `Signal<T>` | Reactive value container with monotonic version tracking |
| `Memo<T>` | Lazy, auto-tracking computed signal |
| `SignalMap<T, U, F>` | Lightweight read-only projection (no subscription) |
| `SignalChangedFuture<T>` | Future that resolves on next mutation |
| `MapChangedFuture<T, U, F>` | Transforms each new value |
| `FilterChangedFuture<T, F>` | Yields only when a predicate matches |
| `batch(f)` | Run `f` with deferred notifications — multiple sets, one notification |
| `in_batch()` | Check if currently inside a batch |
| `memo!(a, b => expr)` | Convenience macro: auto-clones captured signals before the `move` closure |
| `Signal::version()` | Return the current monotonic version number |
| `Memo::is_dirty()` | Check whether a source has changed without triggering computation |
| `Memo::compute_count()` | Number of successful recomputations (including initial) |

## Quick Start

```rust
use auralis_signal::{Signal, Memo, batch};

// Signal
let count = Signal::new(0);
count.set(1);
assert_eq!(count.read(), 1);

// Memo (auto-tracking computed value)
let a = Signal::new(2);
let b = Signal::new(3);
let sum = Memo::new(move || a.read() + b.read());
assert_eq!(sum.read(), 5);

// Batch (multiple sets, one notification)
let x = Signal::new(0);
batch(|| {
    x.set(1);
    x.set(2);
    x.set(3);
});
assert_eq!(x.read(), 3);
```

## Key Properties

- **Deferred callback model** — `set()` never invokes subscribers synchronously; callbacks are queued for the next executor flush
- **Proactive waker deregistration** — dropping a `SignalChangedFuture` immediately removes its subscriber from the signal's list, preventing stale-waker accumulation
- **Panic-safe Memo** — if the compute function panics, old source subscriptions stay intact; the memo recovers on the next successful `read()`
- **Incremental subscription updates** — during recompute, shared dependencies are kept; only removed/new signals trigger subscribe/unsubscribe
- **Panic-safe batch** — `catch_unwind` around each individual notification in the batch drain

## License

Licensed under either of [MIT](https://github.com/chh-itt/auralis/blob/main/LICENSE) or Apache 2.0 at your option.
