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
    /// The most recent fully-formed change event, if any.
    latest: Rc<RefCell<Option<ChangeEvent>>>,
}

impl ChangeReceiver {
    /// Block until the next change event, or return `false` on timeout.
    ///
    /// Drains any pending events into the internal buffer so that
    /// [`latest_event`](Self::latest_event) returns the most recent one.
    #[must_use]
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let had = self.rx.recv_timeout(timeout).is_ok();
        if had {
            self.drain_internal();
        }
        had
    }

    /// Drain all pending events from the channel, updating the
    /// sequence number and latest-event snapshot.
    fn drain_internal(&self) {
        let mut latest_seq = *self.seq.borrow();
        let mut latest_ev = None;
        while let Ok(ev) = self.rx.try_recv() {
            latest_seq = ev.seq;
            latest_ev = Some(ev);
        }
        *self.seq.borrow_mut() = latest_seq;
        if let Some(ev) = latest_ev {
            *self.latest.borrow_mut() = Some(ev);
        }
    }

    /// Return a clone of the most recent change event, if any.
    #[must_use]
    pub fn latest_event(&self) -> Option<ChangeEvent> {
        self.drain_internal();
        self.latest.borrow().clone()
    }

    /// Drain all pending events and return the latest seq number, or
    /// `None` if no events were pending.
    #[must_use]
    pub fn drain_latest_seq(&self) -> Option<u64> {
        let prev = *self.seq.borrow();
        self.drain_internal();
        let curr = *self.seq.borrow();
        (curr != prev || curr != 0).then_some(curr)
    }

    /// Drain all pending events into a `Timeline`.  Returns `true`
    /// if at least one event was drained.
    pub fn drain_into(&self, timeline: &std::rc::Rc<crate::timeline::Timeline>) -> bool {
        let mut drained = false;
        while let Ok(ev) = self.rx.try_recv() {
            timeline.record_with_identity(ev.seq, ev.addr, ev.version);
            *self.seq.borrow_mut() = ev.seq;
            *self.latest.borrow_mut() = Some(ev);
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
    let latest = Rc::new(RefCell::new(None));
    let latest_clone = Rc::clone(&latest);
    let (tx, rx) = mpsc::channel();
    let t0 = Instant::now();

    let token = add_schedule_observer_with_identity(Box::new(move |addr: usize, version: u64| {
        let s = seq_clone.borrow().wrapping_add(1);
        *seq_clone.borrow_mut() = s;
        let event = ChangeEvent {
            seq: s,
            addr,
            version,
            ms_since_start: {
                #[allow(clippy::cast_possible_truncation)]
                {
                    t0.elapsed().as_millis() as u64
                }
            },
        };
        *latest_clone.borrow_mut() = Some(event.clone());
        let _ = tx.send(event);
    }));

    ChangeReceiver {
        seq,
        rx,
        _token: token,
        latest,
    }
}
