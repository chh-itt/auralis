//! Future types that resolve when a signal's value changes.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::signal::{self, Signal, SubscriberId};

// ---------------------------------------------------------------------------
// SignalChangedFuture
// ---------------------------------------------------------------------------

/// A [`Future`] that completes with the signal's current value on the next
/// mutation.
///
/// # Cancellation safety
///
/// Dropping this future **proactively** removes its subscriber from the
/// signal's internal list.  This prevents stale-subscriber accumulation.
///
/// Created by [`Signal::changed`].
#[must_use = "futures do nothing unless polled"]
pub struct SignalChangedFuture<T> {
    signal: Signal<T>,
    seen_version: u64,
    subscription_id: Option<SubscriberId>,
}

impl<T> SignalChangedFuture<T> {
    /// Create a future that resolves on the next change to `signal`.
    pub fn new(signal: &Signal<T>) -> Self {
        Self {
            signal: signal.clone(),
            seen_version: signal::borrow_state(signal).version,
            subscription_id: None,
        }
    }
}

impl<T: Clone + 'static> Future for SignalChangedFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let version = signal::borrow_state(&this.signal).version;

        if version != this.seen_version {
            this.seen_version = version;
            return Poll::Ready(this.signal.read());
        }

        // Not yet changed — subscribe with a callback that wakes this task.
        if this.subscription_id.is_none() {
            let waker = cx.waker().clone();
            let sub_id = signal::subscribe(
                &this.signal,
                Rc::new(move || {
                    waker.wake_by_ref();
                }),
            );
            this.subscription_id = Some(sub_id);
        }

        Poll::Pending
    }
}

impl<T> Drop for SignalChangedFuture<T> {
    fn drop(&mut self) {
        if let Some(id) = self.subscription_id.take() {
            signal::unsubscribe(&self.signal, id);
        }
    }
}

// ---------------------------------------------------------------------------
// MapChangedFuture
// ---------------------------------------------------------------------------

/// A [`Future`] that transforms each new signal value through a mapping
/// function.
///
/// Created by [`Signal::map_changed`].
#[must_use = "futures do nothing unless polled"]
pub struct MapChangedFuture<T, U, F> {
    signal: Signal<T>,
    seen_version: u64,
    f: F,
    subscription_id: Option<SubscriberId>,
    _phantom: PhantomData<fn() -> U>,
}

impl<T, U, F> MapChangedFuture<T, U, F> {
    /// Create a future that maps each new value of `signal` through `f`.
    pub fn new(signal: &Signal<T>, f: F) -> Self {
        Self {
            signal: signal.clone(),
            seen_version: signal::borrow_state(signal).version,
            f,
            subscription_id: None,
            _phantom: PhantomData,
        }
    }
}

impl<T: Clone + 'static, U, F: Fn(&T) -> U + Unpin> Future for MapChangedFuture<T, U, F> {
    type Output = U;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let version = signal::borrow_state(&this.signal).version;

        if version != this.seen_version {
            this.seen_version = version;
            return Poll::Ready((this.f)(&this.signal.read()));
        }

        if this.subscription_id.is_none() {
            let waker = cx.waker().clone();
            let sub_id = signal::subscribe(
                &this.signal,
                Rc::new(move || {
                    waker.wake_by_ref();
                }),
            );
            this.subscription_id = Some(sub_id);
        }

        Poll::Pending
    }
}

impl<T, U, F> Drop for MapChangedFuture<T, U, F> {
    fn drop(&mut self) {
        if let Some(id) = self.subscription_id.take() {
            signal::unsubscribe(&self.signal, id);
        }
    }
}

// ---------------------------------------------------------------------------
// FilterChangedFuture
// ---------------------------------------------------------------------------

/// A [`Future`] that yields a new signal value only when it satisfies a
/// predicate.
///
/// If the predicate returns `false` the future re-waits for the next
/// change instead of resolving.
///
/// Created by [`Signal::filter_changed`].
#[must_use = "futures do nothing unless polled"]
pub struct FilterChangedFuture<T, F> {
    signal: Signal<T>,
    seen_version: u64,
    predicate: F,
    subscription_id: Option<SubscriberId>,
}

impl<T, F> FilterChangedFuture<T, F> {
    /// Create a future that yields when `signal` changes and the new
    /// value satisfies `predicate`.
    pub fn new(signal: &Signal<T>, predicate: F) -> Self {
        Self {
            signal: signal.clone(),
            seen_version: signal::borrow_state(signal).version,
            predicate,
            subscription_id: None,
        }
    }
}

impl<T: Clone + 'static, F: Fn(&T) -> bool + Unpin> Future for FilterChangedFuture<T, F> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let version = signal::borrow_state(&this.signal).version;

        if version != this.seen_version {
            this.seen_version = version;
            let value = this.signal.read();
            if (this.predicate)(&value) {
                return Poll::Ready(value);
            }
            // Predicate failed — fall through to wait for next change.
        }

        if this.subscription_id.is_none() {
            let waker = cx.waker().clone();
            let sub_id = signal::subscribe(
                &this.signal,
                Rc::new(move || {
                    waker.wake_by_ref();
                }),
            );
            this.subscription_id = Some(sub_id);
        }

        Poll::Pending
    }
}

impl<T, F> Drop for FilterChangedFuture<T, F> {
    fn drop(&mut self) {
        if let Some(id) = self.subscription_id.take() {
            signal::unsubscribe(&self.signal, id);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "future_tests.rs"]
mod tests;
