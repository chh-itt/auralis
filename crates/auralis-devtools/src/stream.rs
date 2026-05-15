//! Real-time change stream — identity-aware observer hooks
//! delivered through an mpsc channel.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use auralis_signal::{add_schedule_observer_with_identity, ObserverToken};
use serde::Serialize;

/// A structured change event carrying signal identity.
#[derive(Debug, Clone, Serialize)]
pub struct ChangeEvent {
    /// Monotonic event counter.
    pub seq: u64,
    /// Opaque address of the signal that changed.
    pub addr: usize,
    /// New version number of the signal.
    pub version: u64,
    /// Milliseconds since the stream was created.
    pub ms_since_start: u64,
}

/// Receives change events via an mpsc channel.
pub struct ChangeReceiver {
    seq: Rc<RefCell<u64>>,
    rx: mpsc::Receiver<ChangeEvent>,
    _token: ObserverToken,
}

impl ChangeReceiver {
    /// Block until the next change event, or return `false` on timeout.
    #[must_use]
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        self.rx.recv_timeout(timeout).is_ok()
    }

    /// Drain all pending events and return the latest seq, or `None` if no events.
    #[must_use]
    pub fn drain_latest_seq(&self) -> Option<u64> {
        let mut latest = None;
        while let Ok(ev) = self.rx.try_recv() {
            latest = Some(ev.seq);
        }
        *self.seq.borrow_mut() = latest.unwrap_or(0);
        latest
    }

    /// Drain all pending events into a `Timeline`.  Returns `true`
    /// if at least one event was drained.
    pub fn drain_into(&self, timeline: &std::rc::Rc<crate::timeline::Timeline>) -> bool {
        let mut drained = false;
        while let Ok(ev) = self.rx.try_recv() {
            timeline.record_with_identity(ev.seq, ev.addr, ev.version);
            *self.seq.borrow_mut() = ev.seq;
            drained = true;
        }
        drained
    }

    /// Return the current sequence number.
    #[must_use]
    pub fn current_seq(&self) -> u64 {
        *self.seq.borrow()
    }
}

/// Create a change stream using an identity-aware observer.
#[must_use]
pub fn change_stream() -> ChangeReceiver {
    let seq = Rc::new(RefCell::new(0u64));
    let seq_clone = Rc::clone(&seq);
    let (tx, rx) = mpsc::channel();
    let t0 = Instant::now();

    let token = add_schedule_observer_with_identity(Box::new(move |addr: usize, version: u64| {
        let s = seq_clone.borrow().wrapping_add(1);
        *seq_clone.borrow_mut() = s;
        let _ = tx.send(ChangeEvent {
            seq: s,
            addr,
            version,
            ms_since_start: {
                #[allow(clippy::cast_possible_truncation)]
                {
                    t0.elapsed().as_millis() as u64
                }
            },
        });
    }));

    ChangeReceiver {
        seq,
        rx,
        _token: token,
    }
}
