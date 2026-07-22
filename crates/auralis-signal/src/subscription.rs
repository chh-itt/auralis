//! RAII subscription handle that unsubscribes on drop.
//!
//! Pairs with [`Signal::subscribe`](crate::Signal::subscribe) so that
//! callers don't need to manually track [`SubscriberId`]s and call
//! [`unsubscribe`](crate::unsubscribe).  Dropping the handle (explicitly
//! or via `Element` destruction) cancels the subscription.

use std::cell::Cell;
use std::rc::Rc;

use crate::signal::{subscribe, unsubscribe};
use crate::Signal;

/// RAII guard that calls [`unsubscribe`] on drop.
///
/// Created by [`subscribe_to`].  Multiple handles can be stored in a
/// `Vec` and all cleaned up at once.
///
/// # Example
///
/// ```ignore
/// let handle = subscribe_to(&my_signal, || println!("changed"));
/// // ... later, or on Element drop
/// drop(handle); // unsubscribes
/// ```
pub struct SubscriptionHandle {
    cleanup: Option<Box<dyn FnOnce()>>,
}

impl SubscriptionHandle {
    fn new(cleanup: impl FnOnce() + 'static) -> Self {
        Self {
            cleanup: Some(Box::new(cleanup)),
        }
    }

    /// Wrap an arbitrary cleanup closure in a handle so it participates in
    /// RAII-based lifecycle management (runs exactly once, on drop).
    ///
    /// Used by burin's implicit-observer bridge to store signal
    /// unsubscribe closures alongside explicit subscription handles.
    pub fn from_cleanup(cleanup: impl FnOnce() + 'static) -> Self {
        Self::new(cleanup)
    }

    /// Returns `true` if the handle is still active (has not been dropped).
    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.cleanup.is_some()
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        if let Some(f) = self.cleanup.take() {
            f();
        }
    }
}

impl std::fmt::Debug for SubscriptionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionHandle")
            .field("alive", &self.cleanup.is_some())
            .finish()
    }
}

/// Subscribe `callback` to `signal` and return a [`SubscriptionHandle`]
/// that unsubscribes on drop.
///
/// `callback` is invoked (via the executor's deferred queue) each time
/// `signal` is mutated.  It should be lightweight — typically just
/// setting a dirty flag.
///
/// The returned handle manages an "alive" flag internally, so that
/// callbacks enqueued before unsubscribe are skipped safely.
///
/// # Example
///
/// ```
/// use auralis_signal::{Signal, subscription::subscribe_to};
///
/// let sig = Signal::new(0);
/// let called = std::rc::Rc::new(std::cell::Cell::new(false));
/// let c = called.clone();
/// let handle = subscribe_to(&sig, move || c.set(true));
/// sig.set(1);
/// // callback fires via executor (or synchronously in tests)
/// drop(handle);
/// ```
pub fn subscribe_to<T: 'static>(
    signal: &Signal<T>,
    callback: impl Fn() + 'static,
) -> SubscriptionHandle {
    let alive = Rc::new(Cell::new(true));
    let alive_flag = Rc::clone(&alive);
    let callback: Rc<dyn Fn()> = Rc::new(move || {
        if alive_flag.get() {
            callback();
        }
    });

    let sub_id = subscribe(signal, callback);
    let signal_clone = signal.clone();
    let cleanup = move || {
        alive.set(false);
        unsubscribe(&signal_clone, sub_id);
    };
    SubscriptionHandle::new(cleanup)
}

/// Like [`subscribe_to`] but accepts a `Box<dyn Fn()>` (for use with
/// trait objects).
#[must_use]
pub fn subscribe_to_dyn<T: 'static>(
    signal: &Signal<T>,
    callback: Box<dyn Fn()>,
) -> SubscriptionHandle {
    let alive = Rc::new(Cell::new(true));
    let alive_flag = Rc::clone(&alive);
    let callback: Rc<dyn Fn()> = Rc::new(move || {
        if alive_flag.get() {
            callback();
        }
    });

    let sub_id = subscribe(signal, callback);
    let signal_clone = signal.clone();
    let cleanup = move || {
        alive.set(false);
        unsubscribe(&signal_clone, sub_id);
    };
    SubscriptionHandle::new(cleanup)
}

/// Subscribe a **derived** signal to its source with automatic lifetime
/// management — the reference-counted alternative to juggling
/// [`SubscriptionHandle`]s.
///
/// `callback` runs on every `source` mutation and receives the upgraded
/// `target`, typically to recompute and [`Signal::set`] the derived
/// value.  The subscription holds only [`WeakSignal`](crate::signal::WeakSignal)
/// references:
///
/// - **No ownership cycle**: neither `source` nor `target` is kept
///   alive by the subscription itself.
/// - **Self-unsubscribing**: on the first `source` notification after
///   the last strong `target` clone is dropped, the callback detects
///   the dead weak and removes itself from `source`'s subscriber list.
/// - **Shared-consumer safe**: while *any* strong clone of `target`
///   exists, updates keep flowing — no single owner can accidentally
///   sever other consumers.
///
/// There is intentionally no returned handle: lifetime *is* the
/// target's reference count.
///
/// # Example
///
/// ```
/// use auralis_signal::{Signal, subscription::subscribe_derived};
///
/// auralis_signal::install_schedule_hook(Box::new(|f| f()));
/// let source = Signal::new(2);
/// let doubled = Signal::new(4);
/// subscribe_derived(&source, &doubled, |d| {
///     // recompute from scratch on every source change
///     d.set(0); // placeholder; real code reads source here
/// });
/// drop(doubled);
/// source.set(3); // callback self-unsubscribes
/// assert_eq!(source.subscriber_count(), 0);
/// ```
pub fn subscribe_derived<S: 'static, T: 'static>(
    source: &Signal<S>,
    target: &Signal<T>,
    callback: impl Fn(&Signal<T>) + 'static,
) {
    let weak_target = target.downgrade();
    let weak_source = source.downgrade();
    // The subscriber id is only known after `subscribe`; the callback
    // needs it for self-removal, so it goes through a shared slot.
    let id_slot: Rc<Cell<Option<crate::SubscriberId>>> = Rc::new(Cell::new(None));
    let slot = Rc::clone(&id_slot);

    let cb: Rc<dyn Fn()> = Rc::new(move || {
        if let Some(target) = weak_target.upgrade() {
            callback(&target);
        } else if let Some(id) = slot.take() {
            // Target died — remove ourselves from the source's list.
            // A dead weak_source means the source is being torn down
            // anyway; nothing to clean.
            if let Some(source) = weak_source.upgrade() {
                unsubscribe(&source, id);
            }
        }
    });

    let id = subscribe(source, cb);
    id_slot.set(Some(id));
}

#[cfg(test)]
#[path = "subscription_tests.rs"]
mod tests;
