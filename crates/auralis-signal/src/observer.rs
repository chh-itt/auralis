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
    /// Pre-populated with old dependencies before compute so that
    /// shared signals are not re-subscribed on every recomputation.
    pub seen: Rc<RefCell<HashSet<SignalKey>>>,
    /// Old dependency keys that were actually re-read during this
    /// compute.  Used together with `new_subs` to determine which
    /// subscriptions to keep after recompute.
    pub re_read: Rc<RefCell<HashSet<SignalKey>>>,
}

thread_local! {
    pub(crate) static OBSERVER: RefCell<Option<ObserverState>> = const { RefCell::new(None) };
}

// ── Public API for UI frameworks ──────────────────────────────────────

/// A guard that uninstalls the active observer when dropped.
///
/// Created by [`install_observer`].  While this guard is alive, every
/// [`Signal::read`](crate::Signal::read) / [`Signal::with`](crate::Signal::with)
/// call auto-subscribes the element (or other consumer) to that signal.
pub struct ObserverGuard {
    _private: (),
}

impl Drop for ObserverGuard {
    fn drop(&mut self) {
        OBSERVER.with(|o| {
            *o.borrow_mut() = None;
        });
    }
}

/// Install an observer that auto-subscribes to all signals read during
/// its lifetime.
///
/// `dirty_callback` is called (with no arguments) whenever any observed
/// signal changes.  `on_subscribe` receives the signal's `state_addr`
/// (`usize`) and a cleanup closure; store these to unsubscribe when the
/// consumer (e.g.  a widget [`Element`]) is removed.
///
/// Returns an [`ObserverGuard`] that uninstalls the observer on drop.
pub fn install_observer(
    dirty_callback: Rc<dyn Fn()>,
    on_subscribe: Rc<dyn Fn(usize, Box<dyn FnOnce()>)>,
) -> ObserverGuard {
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    let seen: Rc<RefCell<HashSet<SignalKey>>> = Rc::new(RefCell::new(HashSet::new()));
    let re_read: Rc<RefCell<HashSet<SignalKey>>> = Rc::new(RefCell::new(HashSet::new()));

    let on_sub = on_subscribe;
    let state = ObserverState {
        dirty_callback,
        on_subscribe: Rc::new(move |key: SignalKey, cleanup: Box<dyn FnOnce()>| {
            on_sub(key.addr(), cleanup);
        }),
        seen,
        re_read,
    };

    OBSERVER.with(|o| {
        *o.borrow_mut() = Some(state);
    });

    ObserverGuard { _private: () }
}
