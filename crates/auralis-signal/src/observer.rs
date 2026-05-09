//! Observer infrastructure for automatic dependency tracking.
//!
//! Used by [`Memo`](crate::Memo) during computation.  When an
//! [`ObserverState`] is installed in the thread-local [`OBSERVER`] slot,
//! every [`Signal::read`](crate::Signal::read) / [`Signal::with`](crate::Signal::with)
//! call auto-subscribes the observer to that signal.

#![allow(clippy::type_complexity)]

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use crate::memo::SignalKey;

/// Installed by [`Memo`](crate::Memo) during computation.
///
/// When set, every [`Signal::read`](crate::Signal::read) /
/// [`Signal::with`](crate::Signal::with) call subscribes the observer's
/// dirty callback to that signal and hands a cleanup closure back via
/// `on_subscribe`.
pub(crate) struct ObserverState {
    /// Callback to mark the observer dirty when a source signal changes.
    pub dirty_callback: Rc<dyn Fn()>,
    /// Called with the signal key and an unsubscribe closure each time
    /// a new source dependency is subscribed.  The observer stores these
    /// for cleanup on drop or for incremental diff during recomputation.
    pub on_subscribe: Rc<dyn Fn(SignalKey, Box<dyn FnOnce()>)>,
    /// Set of already-subscribed signal pointers for deduplication.
    /// Prevents double-subscribing when the same signal is read
    /// multiple times within a single compute invocation.
    pub seen: Rc<RefCell<HashSet<SignalKey>>>,
}

thread_local! {
    pub(crate) static OBSERVER: RefCell<Option<ObserverState>> = const { RefCell::new(None) };
}
