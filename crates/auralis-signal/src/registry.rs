//! Thread-local registry of live reactive nodes (signals and memos).
//!
//! Each node registers a callback on construction.  The callback
//! captures a [`Weak`] reference so dead nodes are automatically
//! pruned.  [`dump_registry`] is used by `auralis_task`'s
//! `dump_reactive_graph`.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use crate::signal::SignalState;

/// A snapshot of a reactive node's metadata for diagnostic output.
#[derive(Debug, Clone)]
pub struct ReactiveNodeSnapshot {
    /// The label set via `set_label()`, if any.
    pub label: Option<String>,
    /// `"Signal"` or `"Memo"`.
    pub node_type: &'static str,
    /// Current version number.
    pub version: u64,
    /// Number of active subscriber callbacks.
    pub subscriber_count: usize,
    /// Opaque identity (Rc pointer address).
    pub state_addr: usize,
    /// `true` if the memo is waiting for recomputation.
    pub is_dirty: Option<bool>,
    /// Number of successful recomputations.
    pub compute_count: Option<u64>,
    /// Number of source signal dependencies.
    pub dependency_count: Option<usize>,
}

type RegistryCallback = Box<dyn Fn() -> Option<ReactiveNodeSnapshot>>;

thread_local! {
    static REACTIVE_REGISTRY: RefCell<Vec<RegistryCallback>> = RefCell::new(Vec::new());
}

/// Register a node callback.  Called by [`Signal::new`] and [`Memo::new`].
pub(crate) fn register(cb: RegistryCallback) {
    REACTIVE_REGISTRY.with(|reg| reg.borrow_mut().push(cb));
}

/// Return snapshots of all currently-live reactive nodes.
///
/// Dead nodes (those whose [`Weak`] references can no longer be
/// upgraded) are automatically filtered out and pruned from the
/// registry.
#[must_use]
pub fn dump_registry() -> Vec<ReactiveNodeSnapshot> {
    REACTIVE_REGISTRY.with(|reg| {
        let mut snapshots = Vec::new();
        let mut alive: Vec<RegistryCallback> = Vec::new();

        for cb in reg.borrow_mut().drain(..) {
            if let Some(snap) = cb() {
                snapshots.push(snap);
                alive.push(cb);
            }
        }

        *reg.borrow_mut() = alive;
        snapshots
    })
}

/// Build a callback that captures a [`Weak`] pointer to a signal's state
/// and produces a snapshot when the signal is still live.
pub(crate) fn make_signal_callback<T: 'static>(
    weak: Weak<RefCell<SignalState<T>>>,
    label: Rc<RefCell<Option<String>>>,
    state_addr: usize,
) -> RegistryCallback {
    Box::new(move || {
        let state = weak.upgrade()?;
        let s = state.borrow();
        Some(ReactiveNodeSnapshot {
            label: label.borrow().clone(),
            node_type: "Signal",
            version: s.version,
            subscriber_count: s.subscribers.len(),
            state_addr,
            is_dirty: None,
            compute_count: None,
            dependency_count: None,
        })
    })
}

/// Build a callback for a [`Memo`](crate::Memo) that produces a snapshot
/// when the memo is still live.
pub(crate) fn make_memo_callback<T: 'static>(
    weak_subs: Weak<RefCell<Vec<(crate::memo::SignalKey, crate::memo::CleanupFn)>>>,
    dirty: Rc<std::cell::Cell<bool>>,
    compute_count: Rc<std::cell::Cell<u64>>,
    label: Rc<RefCell<Option<String>>>,
    signal: crate::Signal<T>,
) -> RegistryCallback {
    Box::new(move || {
        let subs = weak_subs.upgrade()?;
        let dep_count = subs.borrow().len();
        let version = signal.version();
        let subscriber_count = signal.subscriber_count();
        Some(ReactiveNodeSnapshot {
            label: label.borrow().clone(),
            node_type: "Memo",
            version,
            subscriber_count,
            state_addr: signal.state_addr(),
            is_dirty: Some(dirty.get()),
            compute_count: Some(compute_count.get()),
            dependency_count: Some(dep_count),
        })
    })
}
