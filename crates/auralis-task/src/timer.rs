//! Timer primitives for the Auralis executor.
//!
//! Provides [`sleep`], an async delay future that cooperatively yields
//! to the executor for the given duration.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::executor;

/// A future that completes after a given duration.
///
/// Created by [`sleep`].  On first poll it registers a timer with the
/// executor and computes a deadline.  Subsequent polls check the
/// deadline — this makes the future robust against premature re-polls
/// (e.g. from `select!` or executor wake-ups unrelated to the timer).
pub struct SleepFuture {
    registered: bool,
    duration_ms: u64,
    /// Deadline in milliseconds, computed on first poll.
    deadline_ms: u64,
}

impl SleepFuture {
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn new(duration: Duration) -> Self {
        Self {
            registered: false,
            duration_ms: duration.as_millis() as u64,
            deadline_ms: 0,
        }
    }
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.registered {
            let now = executor::current_time_ms();
            // Without a TimeSource (now == 0), all timers fire on each
            // flush; treat as expired.
            if now == 0 || now >= self.deadline_ms {
                return Poll::Ready(());
            }
            return Poll::Pending;
        }
        self.registered = true;

        // Discover our task id from the executor's thread-local.
        let task_id = executor::with_current_polling_task(|id| {
            id.expect("timer::sleep must be called from within an auralis task")
        });

        let now = executor::current_time_ms();
        self.deadline_ms = now.saturating_add(self.duration_ms);

        // Return immediately for zero-duration sleeps or when the
        // deadline has already passed (a TimeSource advanced past it).
        if self.duration_ms == 0 || (now > 0 && self.deadline_ms <= now) {
            return Poll::Ready(());
        }

        executor::Executor::schedule_timer(
            &executor::current_executor_instance(),
            self.deadline_ms,
            task_id,
        );

        // Don't self-wake — the executor will re-poll this task when
        // the timer expires (flush step 0).  Without a TimeSource all
        // timers expire on the next flush (equivalent to yield_now).
        Poll::Pending
    }
}

/// Pause the current task for at least `duration`.
///
/// # Panics
///
/// Panics if called outside of an auralis task context (i.e. not from
/// within a future spawned via [`TaskScope::spawn`](crate::TaskScope::spawn)
/// or [`spawn_global`](crate::spawn_global)).
///
/// # Example
///
/// ```rust,ignore
/// use std::time::Duration;
/// use auralis_task::timer;
///
/// scope.spawn(async {
///     timer::sleep(Duration::from_millis(500)).await;
///     // ... do something after delay
/// });
/// ```
pub async fn sleep(duration: Duration) {
    SleepFuture::new(duration).await;
}
