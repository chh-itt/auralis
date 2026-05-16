//! Built-in [`ScheduleFlush`] implementations.
//!
//! Before this module existed, every application had to implement
//! [`ScheduleFlush`] by hand (~15 lines of boilerplate).  Now you can
//! pick [`DeferredScheduler`] for production (buffered, needs a drain
//! loop) or use [`auralis_devtools::init`] for zero-setup diagnostics.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ScheduleFlush;

/// A [`ScheduleFlush`] that buffers callbacks and drains them when
/// [`drain`](Self::drain) is called.
///
/// This is the recommended scheduler for CLI / native applications.
/// Call [`drain`](Self::drain) in your main loop to process pending
/// signal notifications and timer expirations.
///
/// # Example
///
/// ```rust,ignore
/// use auralis_task::scheduler::DeferredScheduler;
/// use auralis_task::init_flush_scheduler;
///
/// let sched = DeferredScheduler::new();
/// init_flush_scheduler(sched.clone());
///
/// // In your main loop:
/// loop {
///     // ... application logic ...
///     sched.drain();
/// }
/// ```
pub struct DeferredScheduler {
    pending: RefCell<Vec<Box<dyn FnOnce()>>>,
}

impl DeferredScheduler {
    /// Create a new deferred scheduler.
    #[must_use]
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            pending: RefCell::new(Vec::new()),
        })
    }

    /// Drain all pending flush callbacks.
    ///
    /// Call this periodically (e.g. once per frame / loop iteration)
    /// to process deferred signal notifications, timer expirations,
    /// and task polls.
    pub fn drain(&self) {
        let cbs: Vec<Box<dyn FnOnce()>> = self.pending.borrow_mut().drain(..).collect();
        for cb in cbs {
            cb();
        }
    }

    /// Return the number of pending callbacks.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.borrow().len()
    }
}

impl Default for DeferredScheduler {
    fn default() -> Self {
        Self {
            pending: RefCell::new(Vec::new()),
        }
    }
}

impl ScheduleFlush for DeferredScheduler {
    fn schedule(&self, callback: Box<dyn FnOnce()>) {
        self.pending.borrow_mut().push(callback);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::*;

    #[test]
    fn deferred_scheduler_buffers_and_drains() {
        let sched = DeferredScheduler::new();
        let calls = Rc::new(Cell::new(0u32));

        let c1 = calls.clone();
        sched.schedule(Box::new(move || c1.set(c1.get() + 1)));
        let c2 = calls.clone();
        sched.schedule(Box::new(move || c2.set(c2.get() + 2)));

        assert_eq!(sched.pending_count(), 2);
        assert_eq!(calls.get(), 0);

        sched.drain();
        assert_eq!(calls.get(), 3);
        assert_eq!(sched.pending_count(), 0);
    }

    #[test]
    fn drain_empty_is_noop() {
        let sched = DeferredScheduler::new();
        sched.drain(); // should not panic
        assert_eq!(sched.pending_count(), 0);
    }

    #[test]
    fn default_scheduler_has_no_pending() {
        let sched = DeferredScheduler::default();
        assert_eq!(sched.pending_count(), 0);
    }

    #[test]
    fn reentrant_schedule_during_drain_deferred_to_next_drain() {
        // Callbacks pushed during drain() must not execute in the same
        // drain cycle — they should be held for the next drain().
        let sched = DeferredScheduler::new();
        let calls = Rc::new(Cell::new(0u32));

        // Use a weak reference so the callback can schedule onto the
        // same scheduler instance.
        let sched_weak = Rc::downgrade(&sched);
        let c1 = calls.clone();
        sched.schedule(Box::new(move || {
            c1.set(1);
            // Re-entrant schedule on the same scheduler during drain.
            if let Some(s) = sched_weak.upgrade() {
                s.schedule(Box::new(|| {}));
            }
        }));

        assert_eq!(sched.pending_count(), 1);
        sched.drain();
        assert_eq!(calls.get(), 1);
        // The re-entrant callback was NOT processed in the current drain
        // (drain(..) took a snapshot before iterating).
        assert_eq!(
            sched.pending_count(),
            1,
            "re-entrant callback should be pending"
        );

        sched.drain();
        assert_eq!(sched.pending_count(), 0);
    }
}
