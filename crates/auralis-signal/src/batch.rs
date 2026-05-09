//! Batch update support.
//!
//! When [`batch`] is active, signal notifications are queued rather than
//! pushed to the executor immediately.  The outermost batch flushes them
//! all at once on exit (including on panic, via [`BatchGuard`]).

use std::cell::{Cell, RefCell};

use crate::signal::executor_schedule;

thread_local! {
    static BATCH_DEPTH: Cell<u32> = const { Cell::new(0) };
    static BATCHED_NOTIFICATIONS: RefCell<Vec<Box<dyn FnOnce()>>> = RefCell::new(Vec::new());
}

pub(crate) fn batch_depth() -> u32 {
    BATCH_DEPTH.with(Cell::get)
}

pub(crate) fn push_batched_notification(f: Box<dyn FnOnce()>) {
    BATCHED_NOTIFICATIONS.with(|cell| cell.borrow_mut().push(f));
}

/// RAII guard that flushes pending notifications when the outermost
/// batch exits, even on panic.
struct BatchGuard;

impl Drop for BatchGuard {
    fn drop(&mut self) {
        BATCH_DEPTH.with(|c| {
            let depth = c.get();
            if depth == 1 {
                c.set(0);
                let notifications: Vec<Box<dyn FnOnce()>> =
                    BATCHED_NOTIFICATIONS.with(|cell| std::mem::take(&mut *cell.borrow_mut()));
                for notification in notifications {
                    executor_schedule(notification);
                }
            } else {
                c.set(depth - 1);
            }
        });
    }
}

/// Run `f` in a **batch** context.
///
/// During the batch, all [`Signal::set`](crate::Signal::set) calls update
/// their values and versions immediately, but subscriber callbacks are
/// deferred until the outermost batch exits.  This eliminates redundant
/// intermediate notifications — if a signal is set multiple times inside a
/// batch, subscribers are only notified once (with the final value).
///
/// # Nested batches
///
/// Nested `batch` calls are supported.  Only the outermost batch flushes
/// pending notifications; inner batches are transparent.
///
/// # Panic safety
///
/// If `f` panics, the batch guard still runs on unwind, resetting the
/// depth and flushing any accumulated notifications.  The batch state
/// is always restored.
///
/// # Example
///
/// ```
/// use auralis_signal::{Signal, batch};
///
/// let a = Signal::new(0);
/// let b = Signal::new(0);
///
/// batch(|| {
///     a.set(1);
///     b.set(2);
///     a.set(3); // only the final value (3) matters for subscribers
/// });
/// // Subscribers of both `a` and `b` are notified exactly once.
/// assert_eq!(a.read(), 3);
/// assert_eq!(b.read(), 2);
/// ```
pub fn batch<F: FnOnce() -> R, R>(f: F) -> R {
    BATCH_DEPTH.with(|c| c.set(c.get() + 1));
    let _guard = BatchGuard;
    f()
}

/// Return `true` if currently inside a [`batch`] context.
#[must_use]
pub fn in_batch() -> bool {
    batch_depth() > 0
}
