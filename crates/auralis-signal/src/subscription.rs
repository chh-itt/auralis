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

#[cfg(test)]
#[path = "subscription_tests.rs"]
mod tests;
