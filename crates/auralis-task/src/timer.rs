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
/// executor; subsequent polls return `Ready` once the deadline has passed.
pub struct SleepFuture {
    registered: bool,
    duration_ms: u64,
}

impl SleepFuture {
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn new(duration: Duration) -> Self {
        Self {
            registered: false,
            duration_ms: duration.as_millis() as u64,
        }
    }
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.registered {
            return Poll::Ready(());
        }
        self.registered = true;

        // Discover our task id from the executor's thread-local.
        let task_id = executor::with_current_polling_task(|id| {
            id.expect("timer::sleep must be called from within an auralis task")
        });

        // If the deadline has already passed (e.g. Duration::ZERO),
        // return immediately without scheduling a timer.
        let now = executor::current_time_ms();
        let deadline = now.saturating_add(self.duration_ms);
        if self.duration_ms == 0 || (now > 0 && deadline <= now) {
            return Poll::Ready(());
        }

        executor::Executor::schedule_timer(
            &executor::current_executor_instance(),
            deadline,
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
