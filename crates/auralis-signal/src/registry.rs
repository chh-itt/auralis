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
///
/// Returned by [`dump_registry`].  Memo-specific fields
/// (`is_dirty`, `compute_count`, `dependency_count`,
/// `dependency_addrs`) are `None` for signals.
#[derive(Debug, Clone)]
pub struct ReactiveNodeSnapshot {
    /// The label set via `set_label()`, or `None` if unlabelled.
    pub label: Option<String>,
    /// `"Signal"` or `"Memo"`.
    pub node_type: &'static str,
    /// Current monotonic version number.
    pub version: u64,
    /// Number of active subscriber callbacks.
    pub subscriber_count: usize,
    /// Opaque identity based on the `Rc` pointer address.
    /// Distinguishes signals that share the same label.
    pub state_addr: usize,
    /// `true` if the memo has pending recomputation (`None` for signals).
    pub is_dirty: Option<bool>,
    /// Number of successful recomputations (`None` for signals).
    pub compute_count: Option<u64>,
    /// Number of source signal dependencies (`None` for signals).
    pub dependency_count: Option<usize>,
    /// Addresses of this memo's source signal dependencies
    /// (`None` for signals, empty if the memo has no deps).
    pub dependency_addrs: Option<Vec<usize>>,
    /// Microseconds spent in the most recent successful recomputation
    /// (`None` for signals).
    pub last_compute_us: Option<u64>,
    /// Total number of times the signal has been mutated
    /// (`None` for memos — use `compute_count` instead).
    pub update_count: Option<u64>,
    /// Rust type name of the stored value, e.g. `"i32"` or
    /// `"alloc::vec::Vec<app::TodoItem>"`.
    pub type_name: Option<String>,
    /// Debug representation of the current value, if a formatter
    /// was set via `Signal::set_value_formatter`.
    pub value_debug: Option<String>,
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
    // Drain callbacks into a temporary vec first so we don't hold
    // the RefCell borrow during callback invocation — a callback
    // that calls `Signal::new` (registering a new node) would
    // otherwise hit a RefCell panic.
    let drained: Vec<RegistryCallback> =
        REACTIVE_REGISTRY.with(|reg| reg.borrow_mut().drain(..).collect());

    let mut snapshots = Vec::new();
    let mut alive: Vec<RegistryCallback> = Vec::new();

    for cb in drained {
        if let Some(snap) = cb() {
            snapshots.push(snap);
            alive.push(cb);
        }
    }

    REACTIVE_REGISTRY.with(|reg| {
        let mut reg = reg.borrow_mut();
        // Merge any callbacks that were registered during callback
        // invocation with the survivors.
        alive.append(&mut *reg);
        *reg = alive;
    });

    snapshots
}

/// Build a callback that captures a [`Weak`] pointer to a signal's state
/// and produces a snapshot when the signal is still live.
pub(crate) fn make_signal_callback<T: 'static>(
    weak: Weak<RefCell<SignalState<T>>>,
    label: Rc<RefCell<Option<String>>>,
    formatter: crate::signal::ValueFormatter<T>,
    state_addr: usize,
) -> RegistryCallback {
    Box::new(move || {
        let state = weak.upgrade()?;
        let s = state.borrow();
        let value_debug = formatter.borrow().as_ref().map(|f| f(&s.value));
        Some(ReactiveNodeSnapshot {
            label: label.borrow().clone(),
            node_type: "Signal",
            version: s.version,
            subscriber_count: s.subscribers.len(),
            state_addr,
            is_dirty: None,
            compute_count: None,
            dependency_count: None,
            dependency_addrs: None,
            last_compute_us: None,
            update_count: Some(s.update_count),
            type_name: Some(std::any::type_name::<T>().to_string()),
            value_debug,
        })
    })
}

/// Build a callback for a [`Memo`](crate::Memo) that produces a snapshot
/// when the memo is still live.
pub(crate) fn make_memo_callback<T: 'static>(
    weak_subs: Weak<RefCell<Vec<(crate::memo::SignalKey, crate::memo::CleanupFn)>>>,
    weak_signal: Weak<RefCell<SignalState<T>>>,
    dirty: Rc<std::cell::Cell<bool>>,
    compute_count: Rc<std::cell::Cell<u64>>,
    label: Rc<RefCell<Option<String>>>,
    last_compute_us: Rc<std::cell::Cell<u64>>,
    state_addr: usize,
) -> RegistryCallback {
    Box::new(move || {
        let subs = weak_subs.upgrade()?;
        let signal_state = weak_signal.upgrade()?;
        let s = signal_state.borrow();
        let sub_list = subs.borrow();
        let dep_count = sub_list.len();
        let dep_addrs: Vec<usize> = sub_list.iter().map(|(key, _)| key.addr()).collect();
        Some(ReactiveNodeSnapshot {
            label: label.borrow().clone(),
            node_type: "Memo",
            version: s.version,
            subscriber_count: s.subscribers.len(),
            state_addr,
            is_dirty: Some(dirty.get()),
            compute_count: Some(compute_count.get()),
            dependency_count: Some(dep_count),
            dependency_addrs: Some(dep_addrs),
            last_compute_us: Some(last_compute_us.get()),
            update_count: None,
            type_name: Some(std::any::type_name::<T>().to_string()),
            value_debug: None,
        })
    })
}
