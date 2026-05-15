//! Real-time change stream based on the observer hook.
//!
//! [`change_stream`] installs a schedule observer and returns a
//! [`ChangeReceiver`].  Every signal mutation increments an internal
//! sequence counter and sends a notification through a rendezvous
//! channel so the consumer can block efficiently via
//! [`wait_timeout`](ChangeReceiver::wait_timeout) instead of
//! busy-polling.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::Duration;

use auralis_signal::{add_schedule_observer, ObserverToken};
use serde::Serialize;

/// A minimal signal-change event emitted by the observer hook.
///
/// The observer fires on every `Signal::set` / `update` / `bump_version`
/// call.  The event carries no payload — consumers should call
/// [`snapshot`](crate::snapshot) to get the full state.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ChangeEvent {
    /// Monotonically increasing event counter for this stream.
    pub seq: u64,
}

/// A receiver for the real-time change stream.
///
/// Created by [`change_stream`].  Use
/// [`wait_timeout`](ChangeReceiver::wait_timeout) to block until the
/// next signal change (or timeout), then call
/// [`current_seq`](ChangeReceiver::current_seq) to read the counter.
pub struct ChangeReceiver {
    seq: Rc<RefCell<u64>>,
    rx: mpsc::Receiver<()>,
    _token: ObserverToken,
}

impl ChangeReceiver {
    /// Return the current change-event sequence number.
    #[must_use]
    pub fn current_seq(&self) -> u64 {
        *self.seq.borrow()
    }

    /// Block until the next signal change, or return `false` on
    /// timeout.
    #[must_use]
    ///
    /// When a signal changes, the internal observer fires and sends a
    /// notification through an `mpsc` channel.  This method receives
    /// that notification (or times out after `timeout`), letting the
    /// caller react without busy-polling.
    ///
    /// A timeout of a few hundred milliseconds provides a natural
    /// heartbeat interval for periodic full snapshots.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        self.rx.recv_timeout(timeout).is_ok()
    }
}

/// Register an observer hook and return a [`ChangeReceiver`].
///
/// The receiver must be kept alive — dropping it removes the
/// observer hook.
///
/// # Example
///
/// ```rust,ignore
/// use std::time::Duration;
/// use auralis_devtools::stream::change_stream;
///
/// let rx = change_stream();
/// loop {
///     rx.wait_timeout(Duration::from_millis(200));
///     let seq = rx.current_seq();
///     // take a snapshot or send a change event
/// }
/// ```
#[must_use]
pub fn change_stream() -> ChangeReceiver {
    let seq = Rc::new(RefCell::new(0u64));
    let seq_clone = Rc::clone(&seq);
    let (tx, rx) = mpsc::channel();

    let token = add_schedule_observer(Box::new(move || {
        *seq_clone.borrow_mut() = seq_clone.borrow().wrapping_add(1);
        // Non-blocking send — if the receiver is not currently
        // waiting, the notification is queued (mpsc internal buffer).
        let _ = tx.send(());
    }));

    ChangeReceiver {
        seq,
        rx,
        _token: token,
    }
}
