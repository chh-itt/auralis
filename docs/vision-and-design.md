# Vision & Design Philosophy

## What Auralis Is

Auralis is an **async-first reactive kernel** distilled to two minimal crates:

- `auralis-signal`: `Signal<T>` + `Memo<T>` — zero dependencies
- `auralis-task`: `TaskScope` + priority executor — depends only on `auralis-signal`

The core idea in one line:

> **Reactive = pausable async tasks. Lifecycle = ownership + structured concurrency.**

## Why It Exists

Traditional reactive programming demands learning a new vocabulary — effects,
cleanups, derived-state graphs, scheduler ticks. Auralis reduces these to
concepts Rust developers already know:

- **`await signal.changed()`** — the task suspends until the value changes
- **`TaskScope` owns tasks** — dropping a scope cancels everything inside it
- **Events / timers / fetch are futures** — compose with `select!`, `join!`

No `on_cleanup` hooks, no manual cancel tokens, no "effect system." Just async
Rust — with cooperative `timer::sleep` for delays.

## Use Cases

Auralis is not specific to Web or Wasm — it fits any scenario involving
"signal change → async task reaction → automatic cleanup":

- **Wasm UI framework kernel** (paired with an existing DOM library)
- **Game engine event systems**
- **IoT sensor data pipelines**
- **Server-side WebSocket connection management**
- **Any asynchronous reactive data pipeline**

## Core Design Decisions

### 1. Deferred Callback Model

`Signal::set` does **not** invoke subscriber callbacks synchronously. Instead,
it pushes a notification closure to the executor's deferred-callback queue,
which is drained at the start of the next flush.

**Why:** This eliminates re-entrancy issues entirely — no
`pending_unsubscriptions`, no `traversal_depth` counters, no set-in-set or
subscribe-during-callback hazards.

**Cost:** One extra microtask of latency. Negligible for virtually all use
cases.

### 2. Proactive Waker Deregistration

When a `SignalChangedFuture` is dropped, **immediately** remove its
subscriber from the signal's internal list — not just lazy cleanup via
an `alive` flag.

**Why:** Prevents "stale waker accumulation" — the most common class of
bugs in async reactive systems. Without proactive cleanup, wakers
accumulate in subscriber lists across rapid create/destroy cycles (e.g.
virtual scrolling), causing memory leaks and degraded performance.

### 3. Single-Threaded by Design (`!Send` / `!Sync`)

`Signal<T>` and `TaskScope` use `Rc<RefCell<...>>` rather than
`Arc<Mutex<...>>`, making them naturally `!Send + !Sync`.

**Why:**

- **Performance:** `Rc` is a simple pointer operation; `RefCell` has no
  atomic overhead. On single-threaded Wasm, `Arc`/`Mutex` atomics are
  pure waste.
- **Safety:** No deadlock risk, no data races.
- **Simplicity:** APIs stay clean without `Send`/`Sync` bound propagation.

**What about multi-threaded scenarios?**

Auralis's escape hatch is **instance isolation**, not shared mutable state:

```rust
// Independent Executor per thread / per request
let ex = Executor::new_instance();
auralis_task::with_executor(&ex, || {
    // Signals and tasks here are routed to `ex`, fully isolated.
});
```

This mirrors tokio's single-threaded runtime + multi-instance model, or
Erlang's lightweight process + message-passing model.

For simpler use-cases (e.g. a worker thread feeding data to a signal),
an `std::sync::mpsc` channel + drain loop is six lines of code — see
`examples/multi_thread_bridge.rs` in `auralis-task`.

**We will not** replace `Rc` with `Arc` or `RefCell` with `Mutex` — that
would sacrifice real performance for the appearance of generality.

### 4. Memo Lazy Evaluation + Single Compute

`Memo::new(compute)` installs an observer and runs `compute` **exactly once**,
simultaneously producing the initial value and discovering dependencies.

When a source signal changes, the memo only sets a dirty flag (one `Cell`
assignment). Actual recomputation is deferred until `read()` or `with()` is
called.

**Why:** Lazy evaluation avoids redundant work (two source changes → one
recomputation). Single compute avoids user dependency on the "pure function"
assumption.

### 5. Panic-Safe Memo Recompute

If `compute` panics during recomputation, the memo keeps its **previous**
subscriptions alive. It stays connected to its source signals and will
recover on the next successful `read()`.

**Why:** Without this guarantee, a single panic would leave the memo
"deaf" — disconnected from all sources, unable to react to changes, and
blocking all downstream watchers.

### 6. Iterative Scope Cancellation (BFS + Leaf-to-Root)

When a `TaskScope` is dropped, descendants are collected via BFS and
cancelled leaf-to-root. Recursion is never used, preventing stack
overflow on deeply nested trees (200+ levels).

`CallbackHandle`s are dropped before tasks, ensuring signal subscriptions
are removed before any task-affecting cleanup.

## What We Don't Do

- **No DOM / rendering layer.** Auralis is a pure kernel; pair it with
  any DOM library.
- **No UI macros.** `view!`, `#[component]`, etc. are out of scope (only minimal
  convenience macros like `provide_context!` / `consume_context!` exist).
- **No router / CLI.** Those belong in higher-level frameworks.
- **No `Send`/`Sync`.** Instance isolation is the recommended multi-threaded
  approach.
