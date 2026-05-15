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
    notifying: bool,
    /// Number of times this signal has been mutated (set/update/bump).
    pub(crate) update_count: u64,
}

/// Type alias for the optional value formatter closure.
pub(crate) type ValueFormatter<T> = Rc<RefCell<Option<Box<dyn Fn(&T) -> String>>>>;

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
    label: Rc<RefCell<Option<String>>>,
    /// Optional closure for formatting the current value as a
    /// debug string.  Set via [`set_value_formatter`](Signal::set_value_formatter).
    value_formatter: ValueFormatter<T>,
}

// Without the diagnostics feature, Signal::new has no 'static bound.
#[cfg(not(feature = "diagnostics"))]
impl<T> Signal<T> {
    /// Create a new signal with the given initial value.
    ///
    /// The initial version is 0.
    #[must_use]
    pub fn new(val: T) -> Self {
        let state = Rc::new(RefCell::new(SignalState {
            value: val,
            version: 0,
            next_subscriber_id: 0,
            subscribers: Vec::new(),
            dirty: false,
            notifying: false,
            update_count: 0,
        }));
        Self {
            state,
            label: Rc::new(RefCell::new(None)),
            value_formatter: Rc::new(RefCell::new(None)),
        }
    }
}

// With the diagnostics feature, registration requires T: 'static.
#[cfg(feature = "diagnostics")]
impl<T: 'static> Signal<T> {
    /// Create a new signal with the given initial value.
    ///
    /// The initial version is 0.
    #[must_use]
    pub fn new(val: T) -> Self {
        let state = Rc::new(RefCell::new(SignalState {
            value: val,
            version: 0,
            next_subscriber_id: 0,
            subscribers: Vec::new(),
            dirty: false,
            notifying: false,
            update_count: 0,
        }));
        let label = Rc::new(RefCell::new(None));
        let value_formatter = Rc::new(RefCell::new(None));

        let weak = Rc::downgrade(&state);
        let addr = Rc::as_ptr(&state) as usize;
        crate::registry::register(crate::registry::make_signal_callback(
            weak,
            Rc::clone(&label),
            Rc::clone(&value_formatter),
            addr,
        ));

        Self {
            state,
            label,
            value_formatter: Rc::new(RefCell::new(None)),
        }
    }
}

impl<T> Signal<T> {
    /// Create a signal **without** registering in the diagnostics
    /// registry.
    #[must_use]
    pub(crate) fn new_untracked(val: T) -> Self {
        Self {
            state: Rc::new(RefCell::new(SignalState {
                value: val,
                version: 0,
                next_subscriber_id: 0,
                subscribers: Vec::new(),
                dirty: false,
                notifying: false,
                update_count: 0,
            })),
            label: Rc::new(RefCell::new(None)),
            value_formatter: Rc::new(RefCell::new(None)),
        }
    }
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

    /// Return a clone of the current value **without** subscribing the
    /// active observer (e.g. a [`Memo`](crate::Memo)) to this signal.
    ///
    /// Use this inside a Memo compute function when reading a signal that
    /// should NOT trigger recomputation when it changes — for example, a
    /// configuration value that is read once during setup, or a signal
    /// that is only conditionally relevant.
    #[must_use]
    pub fn read_untracked(&self) -> T
    where
        T: Clone + 'static,
    {
        self.state.borrow().value.clone()
    }

    /// Borrow the current value immutably and pass it to a closure,
    /// **without** subscribing the active observer.
    ///
    /// The untracked counterpart of [`with`](Signal::with).
    #[must_use]
    pub fn with_untracked<U>(&self, f: impl FnOnce(&T) -> U) -> U
    where
        T: 'static,
    {
        f(&self.state.borrow().value)
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
        state.update_count = state.update_count.wrapping_add(1);
        let addr = Rc::as_ptr(&self.state) as usize;
        let ver = state.version;
        notify_schedule_observers(addr, ver);
        let subs = Self::prepare_notification(&mut state);
        drop(state);
        if let Some(subs) = subs {
            Self::schedule_notification(&self.state, subs);
        }
    }

    /// Internal: notify all subscribers of the given signal state.
    /// Called from the deferred callback queue and from re-entrant
    /// follow-up notifications.
    ///
    /// Unlike [`schedule_notification`](Self::schedule_notification) which
    /// uses a snapshot taken at `set()`-time, this method takes a **fresh**
    /// subscriber snapshot from the current subscriber list.  This is
    /// intentional: a follow-up notification represents a *new* logical
    /// change (from a re-entrant `set()` during the previous callback
    /// round), so subscribers added during that round should be included.
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

    /// Mutate the stored value in-place via a closure, then bump the
    /// version and schedule subscriber callbacks.
    ///
    /// This avoids cloning the previous value during a read-modify-write
    /// cycle.  For a `Signal<Vec<T>>`:
    ///
    /// ```ignore
    /// // Without update(): two clones of the Vec
    /// let mut v = sig.read();
    /// v.push(item);
    /// sig.set(v);
    ///
    /// // With update(): zero clones
    /// sig.update(|v| v.push(item));
    /// ```
    ///
    /// # Panics
    ///
    /// If `f` panics the version is **not** bumped (version bump happens
    /// after `f` returns).  The stored value may be in a partially
    /// mutated state; the signal remains consistent (no broken invariants)
    /// but callers should avoid panicking closures.
    pub fn update(&self, f: impl FnOnce(&mut T))
    where
        T: 'static,
    {
        let mut state = self.state.borrow_mut();
        f(&mut state.value);
        state.version = state.version.wrapping_add(1);
        state.update_count = state.update_count.wrapping_add(1);
        let addr = Rc::as_ptr(&self.state) as usize;
        let ver = state.version;
        notify_schedule_observers(addr, ver);
        let subs = Self::prepare_notification(&mut state);
        drop(state);
        if let Some(subs) = subs {
            Self::schedule_notification(&self.state, subs);
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

    /// Set a human-readable label for this signal.
    ///
    /// Labels appear in `dump_reactive_graph()` output and are useful
    /// for debugging.  Multiple signals can share the same label.
    ///
    /// # Example
    ///
    /// ```
    /// use auralis_signal::Signal;
    ///
    /// let sig = Signal::new(0);
    /// sig.set_label("counter");
    /// assert_eq!(sig.label(), Some("counter".to_string()));
    /// ```
    pub fn set_label(&self, label: impl Into<String>) {
        *self.label.borrow_mut() = Some(label.into());
    }

    /// Return the label set by [`set_label`](Self::set_label), if any.
    #[must_use]
    pub fn label(&self) -> Option<String> {
        self.label.borrow().clone()
    }

    /// Install a closure that formats the current value as a debug
    /// string for `DevTools` snapshots.
    ///
    /// Without this, `value_debug` in [`ReactiveNodeSnapshot`] is `None`.
    ///
    /// # Example
    ///
    /// ```
    /// use auralis_signal::Signal;
    /// let sig = Signal::new(vec![1, 2, 3]);
    /// sig.set_value_formatter(|v| format!("[{} items]", v.len()));
    /// ```
    pub fn set_value_formatter(&self, f: impl Fn(&T) -> String + 'static) {
        *self.value_formatter.borrow_mut() = Some(Box::new(f));
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
        state.update_count = state.update_count.wrapping_add(1);
        let addr = Rc::as_ptr(&self.state) as usize;
        let ver = state.version;
        notify_schedule_observers(addr, ver);
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
            label: Rc::clone(&self.label),
            value_formatter: Rc::clone(&self.value_formatter),
        }
    }
}

/// Formats the signal for debugging, showing the current value,
/// version, and subscriber count.
///
/// **Panics** if `T`'s [`Debug`] implementation calls [`set`](Signal::set)
/// (or any method that borrows `self` mutably) on the same signal,
/// because the formatter already holds an immutable [`RefCell`] borrow.
impl<T: fmt::Debug> fmt::Debug for Signal<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.borrow();
        let mut ds = f.debug_struct("Signal");
        if let Some(ref label) = self.label.borrow().as_ref() {
            ds.field("label", label);
        }
        ds.field("value", &state.value)
            .field("version", &state.version)
            .field("subscribers", &state.subscribers.len())
            .finish_non_exhaustive()
    }
}

#[cfg(not(feature = "diagnostics"))]
impl<T: Default> Default for Signal<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

#[cfg(feature = "diagnostics")]
impl<T: Default + 'static> Default for Signal<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

// ---------------------------------------------------------------------------
// SignalMap — lightweight read-only projection
// ---------------------------------------------------------------------------

/// A lightweight **unidirectional** read-only projection of a [`Signal`].
///
/// `SignalMap` propagates source changes to the mapped value but does
/// **not** write back — setting the source signal is the only way to
/// change the mapped value.  This is intentional: mapping functions
/// are often non-invertible (e.g. `|v| v.len()`), so automatic
/// bidirectional binding would be incorrect in the general case.
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
    /// source signal mutation.
    ///
    /// Note that this reads the signal *after* [`changed`](Signal::changed)
    /// resolves — if a second mutation occurs between the resolve and the
    /// read, the returned value reflects the latest state (not necessarily
    /// the value that triggered the wakeup).  This is a deliberate
    /// trade-off: the caller always gets the freshest value.  If you need
    /// the exact value that triggered the change, use
    /// [`Signal::map_changed`](Signal::map_changed) instead.
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

/// Opaque token returned by [`add_schedule_observer`].  Pass it to
/// [`remove_schedule_observer`] to deregister.
#[derive(Debug, Clone, Copy)]
pub struct ObserverToken {
    index: usize,
    generation: u64,
}

enum ObserverFn {
    /// Legacy no-arg observer.
    Legacy(Box<dyn Fn()>),
    /// Identity-aware observer: receives the mutated signal's
    /// `state_addr` and new version.
    Identity(Box<dyn Fn(usize, u64)>),
}

struct ObserverSlot {
    observer: Option<ObserverFn>,
    generation: u64,
}

thread_local! {
    /// Primary hook — installed by the executor.
    static SCHEDULE_FN: RefCell<Option<Box<dyn Fn(Box<dyn FnOnce()>)>>> = RefCell::new(None);

    /// Observer hooks — notified (with no arguments) whenever a
    /// signal mutation occurs.  Multiple observers can coexist.
    /// Used by DevTools, logging, etc.
    static NOTIFY_OBSERVERS: RefCell<Vec<ObserverSlot>> = const { RefCell::new(Vec::new()) };
}

/// Install the executor's schedule-callback hook (primary, single consumer).
#[doc(hidden)]
pub fn install_schedule_hook(hook: Box<dyn Fn(Box<dyn FnOnce()>)>) {
    SCHEDULE_FN.with(|cell| {
        *cell.borrow_mut() = Some(hook);
    });
}

/// Remove the primary schedule hook (for test teardown).
#[doc(hidden)]
pub fn remove_schedule_hook() {
    SCHEDULE_FN.with(|cell| {
        *cell.borrow_mut() = None;
    });
    NOTIFY_OBSERVERS.with(|cell| {
        cell.borrow_mut().clear();
    });
}

/// Add an observer that is called (with no arguments) whenever a
/// [`Signal::set`](crate::Signal::set) schedules a subscriber
/// notification.
///
/// This is a **passive** observer — it cannot intercept or modify the
/// notification, and it does not receive the callback itself.  For the
/// primary consumer hook (used by the executor), see
/// [`install_schedule_hook`].
///
/// Returns an [`ObserverToken`] for [`remove_schedule_observer`].
///
/// # Re-entrancy
///
/// Adding or removing observers from **inside** an observer callback
/// will panic — the observer list is already borrowed during
/// notification.  Observers should be lightweight (set a flag, log a
/// message) and should not mutate the observer list.
///
/// # Example
///
/// ```
/// use auralis_signal::add_schedule_observer;
///
/// let token = add_schedule_observer(Box::new(|| {
///     // a signal changed — refresh the DevTools panel
/// }));
/// ```
#[must_use]
pub fn add_schedule_observer(observer: Box<dyn Fn()>) -> ObserverToken {
    add_observer(ObserverFn::Legacy(observer))
}

/// Like [`add_schedule_observer`], but the observer receives the
/// mutated signal's `state_addr` and new version number.
#[must_use]
pub fn add_schedule_observer_with_identity(observer: Box<dyn Fn(usize, u64)>) -> ObserverToken {
    add_observer(ObserverFn::Identity(observer))
}

fn add_observer(f: ObserverFn) -> ObserverToken {
    NOTIFY_OBSERVERS.with(|cell| {
        let mut observers = cell.borrow_mut();
        for (i, slot) in observers.iter_mut().enumerate() {
            if slot.observer.is_none() {
                let gen = slot.generation;
                slot.observer = Some(f);
                return ObserverToken {
                    index: i,
                    generation: gen,
                };
            }
        }
        let idx = observers.len();
        observers.push(ObserverSlot {
            observer: Some(f),
            generation: 0,
        });
        ObserverToken {
            index: idx,
            generation: 0,
        }
    })
}

/// Remove a previously-registered observer.
///
/// The token is **consumed** — calling this a second time with the
/// same token is a no-op (the generation counter is bumped on
/// removal, so the stale token no longer matches).
pub fn remove_schedule_observer(token: ObserverToken) {
    NOTIFY_OBSERVERS.with(|cell| {
        let mut observers = cell.borrow_mut();
        if let Some(slot) = observers.get_mut(token.index) {
            if slot.generation == token.generation && slot.observer.is_some() {
                slot.observer = None;
                slot.generation = slot.generation.wrapping_add(1);
            }
        }
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

thread_local! {
    /// Guard against re-entrant observer notification.  If an observer
    /// callback calls `Signal::set` (triggering another round of
    /// `notify_schedule_observers`), the nested call is a no-op.
    static IN_NOTIFY_OBSERVERS: Cell<bool> = const { Cell::new(false) };
}

struct NotifyGuard;

impl Drop for NotifyGuard {
    fn drop(&mut self) {
        IN_NOTIFY_OBSERVERS.with(|c| c.set(false));
    }
}

/// Notify all passive schedule-observer hooks.
///
/// `addr` and `version` are the mutated signal's identity and new
/// version.  Identity-aware observers receive them; legacy
/// (no-arg) observers are still called for backward compatibility.
fn notify_schedule_observers(addr: usize, version: u64) {
    if IN_NOTIFY_OBSERVERS.with(|c| c.replace(true)) {
        return; // re-entrant — skip
    }
    let _guard = NotifyGuard;

    // Dispatch to all observers with identity info.
    NOTIFY_OBSERVERS.with(|cell| {
        for slot in cell.borrow().iter() {
            match &slot.observer {
                Some(ObserverFn::Legacy(obs)) => {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        obs();
                    }));
                }
                Some(ObserverFn::Identity(obs)) => {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        obs(addr, version);
                    }));
                }
                None => {}
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Timing hook — installable by the host for WASM performance.now()
// ---------------------------------------------------------------------------

thread_local! {
    static TIMING_HOOK: RefCell<Option<fn() -> u64>> = RefCell::new(None);
}

/// Install a microsecond-precision timer hook.
///
/// On native platforms `std::time::Instant` is used by default.
/// On WASM, call this with a function that returns
/// `performance.now() * 1000.0` to enable Memo recompute timing.
pub fn install_timing_hook(hook: fn() -> u64) {
    TIMING_HOOK.with(|c| *c.borrow_mut() = Some(hook));
}

/// Return the current time in microseconds, or 0 if no hook is
/// installed and the platform doesn't support `Instant`.
#[must_use]
pub fn now_us() -> u64 {
    TIMING_HOOK.with(|c| {
        if let Some(hook) = c.borrow().as_ref() {
            return hook();
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            // Use a static base time to get relative microseconds.
            use std::cell::Cell;
            thread_local! {
                static T0: Cell<Option<std::time::Instant>> = const { Cell::new(None) };
            }
            let now = std::time::Instant::now();
            let base = T0.with(|t| {
                t.get().unwrap_or_else(|| {
                    t.set(Some(now));
                    now
                })
            });
            #[allow(clippy::cast_possible_truncation)]
            {
                (now - base).as_micros() as u64
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            0
        }
    })
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
#[path = "signal_tests.rs"]
mod tests;
