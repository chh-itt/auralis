//! Signal state, subscriber management, and the deferred notification
//! state machine.
//!
//! ## Deferred callback model
//!
//! `Signal::set` does **not** invoke subscriber callbacks synchronously.
//! Instead it pushes a notification closure to the executor's
//! deferred-callback queue.  The executor drains this queue at the start
//! of every flush, before polling tasks.  This eliminates re-entrancy
//! issues (set-in-set, subscribe-during-callback, etc.).
//!
//! Each subscriber carries an `alive` flag (`Rc<Cell<bool>>`).  When
//! the notification closure fires it checks the flag before invoking
//! the callback, so unsubscriptions that occur between `set` and the
//! next flush are honoured without touching the subscriber list mid-flight.
//!
//! ## Notification state machine
//!
//! ```text
//!                  set() / bump_version()
//!                        │
//!                        ▼
//!               ┌─────────────────┐
//!               │ prepare_notify  │
//!               │ check flags     │
//!               └───────┬─────────┘
//!                       │
//!           notifying?  │  dirty?
//!           ┌───────────┤  ┌──────────┐
//!           ▼           │  ▼          │
//!       dirty=true      │ return      │ subscribers
//!       return None      │ None        │ empty → None
//!                       │             │
//!                       ▼             ▼
//!               ┌──────────────────────┐
//!               │  dirty=true          │
//!               │  snapshot subscribers│
//!               └──────────┬───────────┘
//!                          │
//!                          ▼
//!               ┌──────────────────────┐
//!               │ schedule_notification│
//!               │ (batch-aware)        │
//!               └──────────┬───────────┘
//!                          │
//!                          ▼
//!               ┌──────────────────────┐
//!               │  notification fires  │
//!               │  notifying=true      │
//!               │  dirty=false         │
//!               │  call subscribers    │
//!               │  ┌─────────────────┐ │
//!               │  │ re-entrant set? │ │
//!               │  │ → dirty=true    │ │
//!               │  └─────────────────┘ │
//!               │  notifying=false     │
//!               │  check dirty         │
//!               └──────────┬───────────┘
//!                          │
//!                   dirty? │
//!                   ┌──────┴──────┐
//!                   ▼             ▼
//!           schedule follow-up   done
//!           (notify_signal_state)
//! ```

#![allow(clippy::type_complexity)]

use std::cell::{Cell, RefCell};
use std::fmt;
use std::rc::Rc;

use crate::batch::{batch_depth, push_batched_notification};
use crate::observer::OBSERVER;

pub(crate) type SubscriberId = u64;

pub(crate) struct Subscriber {
    pub(crate) id: SubscriberId,
    /// Set to `false` when this subscriber is unsubscribed.  The
    /// deferred notification closure checks this flag before calling
    /// `callback`.
    pub(crate) alive: Rc<Cell<bool>>,
    pub(crate) callback: Rc<dyn Fn()>,
}

pub(crate) struct SignalState<T> {
    pub(crate) value: T,
    pub(crate) version: u64,
    pub(crate) next_subscriber_id: SubscriberId,
    pub(crate) subscribers: Vec<Subscriber>,
    /// `true` when a notification for this signal has already been
    /// scheduled for the current microtask.  Subsequent `set` calls
    /// before the notification fires only update `value`/`version`
    /// without scheduling additional notifications.
    dirty: bool,
    /// `true` while a notification is actively invoking callbacks.
    /// Prevents re-entrant `set` from scheduling a new notification
    /// that would be immediately invoked synchronously (infinite loop).
    /// When a callback calls `set` on this signal, the notification
    /// closure detects the stale version at the end and re-schedules.
    notifying: bool,
}

/// A reactive value container with monotonic version tracking.
///
/// Every mutation increments the version, allowing change-detection
/// futures to observe updates efficiently. The signal is cheap to
/// clone (reference-counted) and is single-threaded (`!Send`).
///
/// # Callback execution model
///
/// Subscriber callbacks are **not** invoked during [`set`](Signal::set).
/// Instead they are pushed to the executor's deferred-callback queue
/// and executed at the start of the next flush.  This eliminates
/// re-entrancy hazards and simplifies the internal state machine.
///
/// # Examples
///
/// ```
/// use auralis_signal::Signal;
///
/// let count = Signal::new(0);
/// assert_eq!(count.read(), 0);
/// count.set(42);
/// assert_eq!(count.read(), 42);
/// ```
pub struct Signal<T> {
    pub(crate) state: Rc<RefCell<SignalState<T>>>,
}

impl<T> Signal<T> {
    /// Return an opaque identity token for this signal's allocation.
    ///
    /// Two signals compare equal via this token iff they share the same
    /// internal allocation (i.e. are clones of each other).  The value
    /// is the `Rc` allocation address cast to `usize` — safe because
    /// `Signal<T>` is `!Send + !Sync` (the `Rc` can never migrate to
    /// another thread, so the address is a stable identity).
    pub(crate) fn state_addr(&self) -> usize {
        Rc::as_ptr(&self.state) as usize
    }

    /// Create a new signal with the given initial value.
    ///
    /// The initial version is 0.
    #[must_use]
    pub fn new(val: T) -> Self {
        Self {
            state: Rc::new(RefCell::new(SignalState {
                value: val,
                version: 0,
                next_subscriber_id: 0,
                subscribers: Vec::new(),
                dirty: false,
                notifying: false,
            })),
        }
    }

    /// Return a clone of the current value.
    ///
    /// When called inside a [`Memo`](crate::Memo) compute function, this
    /// auto-subscribes the memo to this signal so that subsequent
    /// mutations mark the memo dirty.
    #[must_use]
    pub fn read(&self) -> T
    where
        T: Clone + 'static,
    {
        let val = self.state.borrow().value.clone();
        track_observer(self);
        val
    }

    /// Replace the stored value, bump the version, and schedule
    /// subscriber callbacks for the next executor flush.
    ///
    /// # Deferred execution
    ///
    /// Callbacks are **not** invoked synchronously.  They are pushed to
    /// the executor's deferred-callback queue and executed at the start
    /// of the next flush cycle.  This guarantees that:
    ///
    /// - Callbacks never observe a partially-updated signal graph.
    /// - Subscribe / unsubscribe during a callback cannot cause
    ///   borrow conflicts.
    /// - Nested / re-entrant `set` calls simply update the value and
    ///   version; the already-scheduled notification will see the
    ///   latest state.
    ///
    /// Unlike [`set_if_changed`](Signal::set_if_changed), this method does
    /// **not** perform value comparison — callbacks are always scheduled,
    /// even when `val` equals the current value.
    pub fn set(&self, val: T)
    where
        T: 'static,
    {
        let mut state = self.state.borrow_mut();
        state.value = val;
        state.version = state.version.wrapping_add(1);
        let subs = Self::prepare_notification(&mut state);
        drop(state);
        if let Some(subs) = subs {
            Self::schedule_notification(&self.state, subs);
        }
    }

    /// Internal: notify all subscribers of the given signal state.
    /// Called from the deferred callback queue and from re-entrant
    /// follow-up notifications.  Takes a fresh subscriber snapshot.
    fn notify_signal_state(state_ref: &Rc<RefCell<SignalState<T>>>)
    where
        T: 'static,
    {
        let subs = {
            let mut state = state_ref.borrow_mut();
            if !state.dirty {
                return;
            }
            state.notifying = true;
            state.dirty = false;
            let s: Vec<(Rc<Cell<bool>>, Rc<dyn Fn()>)> = state
                .subscribers
                .iter()
                .filter(|s| s.alive.get())
                .map(|s| (Rc::clone(&s.alive), Rc::clone(&s.callback)))
                .collect();
            state.subscribers.retain(|s| s.alive.get());
            s
        };

        if Self::notify_and_check_follow_up(state_ref, &subs) {
            let state_ref2 = Rc::clone(state_ref);
            executor_schedule(move || {
                Self::notify_signal_state(&state_ref2);
            });
        }
    }

    /// Call alive subscribers and check whether a re-entrant `set`
    /// during the callbacks requires a follow-up notification.
    ///
    /// Returns `true` if a follow-up should be scheduled.
    fn notify_and_check_follow_up(
        state_ref: &Rc<RefCell<SignalState<T>>>,
        subs: &[(Rc<Cell<bool>>, Rc<dyn Fn()>)],
    ) -> bool {
        for (alive, cb) in subs {
            if alive.get() {
                cb();
            }
        }

        let mut state = state_ref.borrow_mut();
        state.notifying = false;
        state.subscribers.retain(|s| s.alive.get());
        // Don't clear dirty here — if a re-entrant set happened during
        // callbacks, the follow-up notification scheduled by the caller
        // must see dirty = true so it can process the pending change.
        state.dirty
    }

    /// Common pre-flight for `set` and `bump_version`: check the
    /// notifying/dirty flags, snapshot active subscribers, and return
    /// `Some(subs)` if a notification should be scheduled.
    ///
    /// Returns `None` if a notification is already in-flight (re-entrant
    /// set/bump) or already scheduled (dirty flag set).
    fn prepare_notification(
        state: &mut SignalState<T>,
    ) -> Option<Vec<(Rc<Cell<bool>>, Rc<dyn Fn()>)>> {
        if state.notifying {
            state.dirty = true;
            return None;
        }
        if state.dirty {
            return None;
        }
        // Skip allocation when there are no subscribers at all.
        if state.subscribers.is_empty() {
            return None;
        }
        state.dirty = true;
        let subs: Vec<(Rc<Cell<bool>>, Rc<dyn Fn()>)> = state
            .subscribers
            .iter()
            .filter(|s| s.alive.get())
            .map(|s| (Rc::clone(&s.alive), Rc::clone(&s.callback)))
            .collect();
        Some(subs)
    }

    /// Build the deferred notification closure and hand it to the
    /// executor (or batch buffer).  Uses the pre-snapshotted `subs`
    /// list from [`prepare_notification`](Self::prepare_notification)
    /// so that new subscribers added after `set` are not notified
    /// for this change.
    fn schedule_notification(
        state_ref: &Rc<RefCell<SignalState<T>>>,
        subs: Vec<(Rc<Cell<bool>>, Rc<dyn Fn()>)>,
    ) where
        T: 'static,
    {
        let state_ref = Rc::clone(state_ref);
        let notification = move || {
            {
                let mut state = state_ref.borrow_mut();
                state.notifying = true;
                state.dirty = false;
                state.subscribers.retain(|s| s.alive.get());
            }

            if Self::notify_and_check_follow_up(&state_ref, &subs) {
                let state_ref2 = Rc::clone(&state_ref);
                executor_schedule(move || {
                    Self::notify_signal_state(&state_ref2);
                });
            }
        };

        if batch_depth() > 0 {
            push_batched_notification(Box::new(notification));
        } else {
            executor_schedule(notification);
        }
    }

    /// Set the value only if it differs from the current value.
    ///
    /// Compares by reference (`&T == &T`), avoiding a clone of the
    /// stored value.  When the values are equal this is a no-op: no
    /// callbacks are scheduled, no version bump occurs.
    pub fn set_if_changed(&self, val: T)
    where
        T: PartialEq + 'static,
    {
        let changed = self.with(|current| current != &val);
        if changed {
            self.set(val);
        }
    }

    /// Borrow the current value immutably and pass it to a closure.
    ///
    /// This avoids cloning the value when you only need to inspect it
    /// (e.g. checking a flag, reading a length).  For obtaining a
    /// long-lived copy, use [`read`](Signal::read) instead.
    ///
    /// When called inside a [`Memo`](crate::Memo) compute function, this
    /// auto-subscribes the memo to this signal so that subsequent
    /// mutations mark the memo dirty.
    ///
    /// # Panics
    ///
    /// Panics if `f` calls [`set`](Signal::set) on the same signal,
    /// because that would create a `RefCell` borrow conflict.
    ///
    /// # Example
    ///
    /// ```
    /// use auralis_signal::Signal;
    ///
    /// let sig = Signal::new(vec![1, 2, 3]);
    /// let len = sig.with(|v| v.len());
    /// assert_eq!(len, 3);
    /// ```
    pub fn with<U>(&self, f: impl FnOnce(&T) -> U) -> U
    where
        T: 'static,
    {
        let result = f(&self.state.borrow().value);
        track_observer(self);
        result
    }

    /// Create a lightweight read-only projection.
    ///
    /// Unlike [`Memo`](crate::Memo), this does **not** track dependencies
    /// or cache the result — it simply applies `f` on every
    /// [`read`](SignalMap::read) / [`with`](SignalMap::with).
    /// This is suitable for cheap field projections such as
    /// `sig.map(|v| v.len())`.
    ///
    /// # Example
    ///
    /// ```
    /// use auralis_signal::Signal;
    ///
    /// let sig = Signal::new(vec![1, 2, 3]);
    /// let len = sig.map(|v: &Vec<i32>| v.len());
    /// assert_eq!(len.read(), 3);
    /// ```
    pub fn map<U, F>(&self, f: F) -> SignalMap<T, U, F>
    where
        F: Fn(&T) -> U,
    {
        SignalMap {
            source: self.clone(),
            f,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a [`SignalChangedFuture`](crate::SignalChangedFuture) that
    /// resolves with the new value on the next mutation.
    pub fn changed(&self) -> crate::SignalChangedFuture<T> {
        crate::SignalChangedFuture::new(self)
    }

    /// Create a [`MapChangedFuture`](crate::MapChangedFuture) that
    /// transforms each new value through `f`.
    pub fn map_changed<U, F>(&self, f: F) -> crate::MapChangedFuture<T, U, F>
    where
        F: Fn(&T) -> U,
    {
        crate::MapChangedFuture::new(self, f)
    }

    /// Create a [`FilterChangedFuture`](crate::FilterChangedFuture) that
    /// yields only when `f(&new_value)` returns `true`.
    pub fn filter_changed<F>(&self, f: F) -> crate::FilterChangedFuture<T, F>
    where
        F: Fn(&T) -> bool,
    {
        crate::FilterChangedFuture::new(self, f)
    }

    /// Return `true` if `self` and `other` are clones of the same
    /// underlying signal allocation.
    ///
    /// # Example
    ///
    /// ```
    /// use auralis_signal::Signal;
    ///
    /// let a = Signal::new(0);
    /// let b = a.clone();
    /// let c = Signal::new(0);
    /// assert!(a.ptr_eq(&b));
    /// assert!(!a.ptr_eq(&c));
    /// ```
    #[must_use]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.state, &other.state)
    }

    /// Return the number of currently registered subscriber callbacks.
    ///
    /// Useful for runtime diagnostics and leak detection.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.state.borrow().subscribers.len()
    }

    /// Return the current version number.
    ///
    /// The version is incremented (wrapping) on every [`set`](Signal::set)
    /// call.  It can be used to detect mutations without cloning the
    /// stored value.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.state.borrow().version
    }

    /// Return the number of currently registered subscribers.
    ///
    /// This is intended for testing and debugging.
    #[cfg(test)]
    #[must_use]
    pub fn debug_count_waiters(&self) -> usize {
        self.subscriber_count()
    }

    /// Bump the version and schedule subscriber callbacks without
    /// changing the stored value.
    ///
    /// Used by [`Memo`](crate::Memo) to notify its own subscribers when
    /// a source signal has changed but the memo hasn't been recomputed
    /// yet (lazy evaluation).  The version bump ensures that
    /// [`SignalChangedFuture`](crate::SignalChangedFuture) and nested
    /// memos see the change and trigger recomputation on next read.
    pub(crate) fn bump_version(&self)
    where
        T: 'static,
    {
        let mut state = self.state.borrow_mut();
        state.version = state.version.wrapping_add(1);
        let subs = Self::prepare_notification(&mut state);
        drop(state);
        if let Some(subs) = subs {
            Self::schedule_notification(&self.state, subs);
        }
    }
}

impl<T> Clone for Signal<T> {
    fn clone(&self) -> Self {
        Self {
            state: Rc::clone(&self.state),
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Signal<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.borrow();
        f.debug_struct("Signal")
            .field("value", &state.value)
            .field("version", &state.version)
            .field("subscribers", &state.subscribers.len())
            .finish()
    }
}

impl<T: Default> Default for Signal<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

// ---------------------------------------------------------------------------
// SignalMap — lightweight read-only projection
// ---------------------------------------------------------------------------

/// A lightweight read-only projection of a [`Signal`].
///
/// Unlike [`Memo`](crate::Memo), `SignalMap` does **not** track
/// dependencies or cache the result — it simply applies the mapping
/// function on every access.  This is suitable for cheap projections
/// such as `.len()` or field access.
///
/// Created by [`Signal::map`].
pub struct SignalMap<T, U, F> {
    source: Signal<T>,
    f: F,
    _phantom: std::marker::PhantomData<fn() -> U>,
}

impl<T: Clone + 'static, U, F: Fn(&T) -> U> SignalMap<T, U, F> {
    /// Apply the mapping function to the current source value and
    /// return the result.
    #[must_use]
    pub fn read(&self) -> U {
        self.source.with(|v| (self.f)(v))
    }

    /// Borrow the source value and pass a reference to the mapped
    /// result through `g`.  This avoids an intermediate clone of `U`.
    #[must_use]
    pub fn with<R>(&self, g: impl FnOnce(&U) -> R) -> R {
        // Delegate to source.with() so that observer tracking is
        // handled uniformly with read().
        self.source.with(|v| g(&(self.f)(v)))
    }

    /// Return a future that resolves with the mapped value on the next
    /// source signal mutation.  Delegates to the underlying signal's
    /// [`changed`](Signal::changed).
    pub async fn changed(&self) -> U {
        self.source.changed().await;
        self.read()
    }
}

impl<T, U, F: Clone> Clone for SignalMap<T, U, F> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            f: self.f.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// crate-internal helpers used by the future types
// ---------------------------------------------------------------------------

pub(crate) fn borrow_state<T>(sig: &Signal<T>) -> std::cell::Ref<'_, SignalState<T>> {
    sig.state.borrow()
}

/// Register a subscriber callback and return its id.
///
/// The callback is invoked (with no arguments) via the executor's deferred
/// queue on every subsequent [`Signal::set`] call.  It should capture the
/// signal and call `.read()` if it needs the current value.
///
/// # Safety / guarantees
///
/// - If `unsubscribe` is called before a deferred notification fires,
///   the `alive` flag is set to false and the in-flight closure skips
///   the callback.  No double-fire is possible.
/// - Calling `unsubscribe` with a stale or already-unsubscribed id is
///   a no-op.
/// - The returned id is valid until `unsubscribe` is called; it is
///   not recycled.
#[doc(hidden)]
pub fn subscribe<T>(sig: &Signal<T>, callback: Rc<dyn Fn()>) -> SubscriberId {
    let mut state = sig.state.borrow_mut();
    let id = state.next_subscriber_id;
    state.next_subscriber_id = state.next_subscriber_id.wrapping_add(1);
    state.subscribers.push(Subscriber {
        id,
        alive: Rc::new(Cell::new(true)),
        callback,
    });
    id
}

/// Remove a subscriber by id.
///
/// The subscriber is marked dead immediately and removed from the list.
/// Any in-flight deferred notification will skip it (the `alive` flag
/// is checked before each callback invocation).
#[doc(hidden)]
pub fn unsubscribe<T>(sig: &Signal<T>, id: SubscriberId) {
    let mut state = sig.state.borrow_mut();
    if let Some(sub) = state.subscribers.iter().find(|s| s.id == id) {
        sub.alive.set(false);
    }
    state.subscribers.retain(|s| s.id != id);
}

// ---------------------------------------------------------------------------
// Hook point — set by the task executor at init time
// ---------------------------------------------------------------------------

thread_local! {
    static SCHEDULE_FN: RefCell<Option<Box<dyn Fn(Box<dyn FnOnce()>)>>> = RefCell::new(None);
}

/// Install the executor's schedule-callback hook.
///
/// Called once by `auralis_task` during initialisation.  The hook
/// receives a `Box<dyn FnOnce()>` and must push it into the executor's
/// deferred-callback queue.
#[doc(hidden)]
pub fn install_schedule_hook(hook: Box<dyn Fn(Box<dyn FnOnce()>)>) {
    SCHEDULE_FN.with(|cell| {
        *cell.borrow_mut() = Some(hook);
    });
}

/// Remove the schedule hook (for test teardown).
#[doc(hidden)]
pub fn remove_schedule_hook() {
    SCHEDULE_FN.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

pub(crate) fn executor_schedule(f: impl FnOnce() + 'static) {
    SCHEDULE_FN.with(|cell| {
        if let Some(hook) = cell.borrow().as_ref() {
            hook(Box::new(f));
        } else {
            // No executor hook installed — invoke synchronously as
            // a fallback (tests that don't initialise the executor).
            f();
        }
    });
}

// ---------------------------------------------------------------------------
// Observer tracking — called by Signal::read / Signal::with
// ---------------------------------------------------------------------------

/// If an observer is currently active (i.e. we're inside a
/// [`Memo`](crate::Memo) computation), subscribe the observer to
/// `signal` so that future mutations mark the memo dirty.
///
/// # `T: 'static` bound
///
/// The observer stores cleanup closures as `dyn FnOnce() + 'static`.
/// Those closures must capture enough to call `unsubscribe`, which
/// requires a handle to the signal's state (and transitively to `T`).
/// Hence `T: 'static` is a hard requirement inherited by every API
/// that may trigger observer tracking ([`Signal::read`],
/// [`Signal::with`], and the change-detection futures).
fn track_observer<T: 'static>(sig: &Signal<T>) {
    OBSERVER.with(|cell| {
        if let Some(ref observer) = *cell.borrow() {
            let key = crate::memo::SignalKey::new(sig);
            {
                let mut seen = observer.seen.borrow_mut();
                if !seen.insert(key) {
                    // Already in `seen` — this is an old dependency being
                    // re-read.  Record it so the incremental diff knows to
                    // keep its subscription.
                    observer.re_read.borrow_mut().insert(key);
                    return;
                }
            }

            let signal = sig.clone();
            let alive = Rc::new(Cell::new(true));
            let alive_clone = Rc::clone(&alive);
            let dirty_cb = Rc::clone(&observer.dirty_callback);

            let callback: Rc<dyn Fn()> = Rc::new(move || {
                if alive_clone.get() {
                    dirty_cb();
                }
            });

            let id = subscribe(&signal, callback);

            let cleanup = Box::new(move || {
                alive.set(false);
                unsubscribe(&signal, id);
            });

            (observer.on_subscribe)(key, cleanup);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_version_increments_on_set() {
        let sig = Signal::new(0i32);
        assert_eq!(sig.version(), 0);

        sig.set(1);
        assert_eq!(sig.version(), 1);

        sig.set(2);
        assert_eq!(sig.version(), 2);
    }

    #[test]
    fn signal_version_unchanged_without_set() {
        let sig = Signal::new(42);
        let v1 = sig.version();
        // read() does not change version.
        let _ = sig.read();
        assert_eq!(sig.version(), v1);
    }
}
