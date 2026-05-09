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
    pub(crate) fn new(duration: Duration) -> Self {
        Self {
            registered: false,
            duration_ms: duration.as_millis() as u64,
        }
    }
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if !self.registered {
            self.registered = true;

            // Discover our task id from the executor's thread-local.
            let task_id = executor::with_current_polling_task(|id| {
                id.expect("timer::sleep must be called from within an auralis task")
            });

            // Compute deadline and register with the executor.
            let now = executor::current_time_ms();
            let deadline = now.saturating_add(self.duration_ms);

            executor::Executor::schedule_timer(
                &executor::current_executor_instance(),
                deadline,
                task_id,
            );

            // Re-register the waker so the executor can wake us.
            cx.waker().wake_by_ref();

            Poll::Pending
        } else {
            Poll::Ready(())
        }
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
    SleepFuture::new(duration).await
}
