//! Change timeline — a ring buffer of recent signal mutation events.
//!
//! [`Timeline`] records the last N change events with sequence numbers
//! and wall-clock timestamps.
//!
//! Feed it from a [`ChangeReceiver`](crate::stream::change_stream).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Instant;

use serde::Serialize;

/// One entry in the change timeline.
#[derive(Debug, Clone, Serialize)]
pub struct TimelineEntry {
    /// Monotonic sequence number from the change stream.
    pub seq: u64,
    /// Opaque address of the signal that changed, if identity is
    /// available (requires identity-aware observer).
    pub addr: Option<usize>,
    /// New version number of the changed signal.
    pub version: Option<u64>,
    /// Wall-clock instant (not serialized).
    #[serde(skip)]
    pub at: Instant,
    /// Milliseconds since the timeline was created.
    pub ms_since_start: u64,
}

/// A ring buffer of recent change events.
///
/// Created by [`Timeline::new`].
pub struct Timeline {
    entries: RefCell<VecDeque<TimelineEntry>>,
    capacity: usize,
    t0: Instant,
}

impl Timeline {
    /// Create a new timeline with space for `capacity` entries.
    #[must_use]
    pub fn new(capacity: usize) -> Rc<Self> {
        Rc::new(Self {
            entries: RefCell::new(VecDeque::with_capacity(capacity)),
            capacity,
            t0: Instant::now(),
        })
    }

    /// Record a change event (no identity).
    pub fn record(&self, seq: u64) {
        self.record_event(seq, None, None);
    }

    /// Record a change with signal identity (`addr`, `new_version`).
    pub fn record_with_identity(&self, seq: u64, addr: usize, version: u64) {
        self.record_event(seq, Some(addr), Some(version));
    }

    fn record_event(&self, seq: u64, addr: Option<usize>, version: Option<u64>) {
        let now = Instant::now();
        let mut entries = self.entries.borrow_mut();
        if entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(TimelineEntry {
            seq,
            addr,
            version,
            at: now,
            ms_since_start: {
                #[allow(clippy::cast_possible_truncation)]
                {
                    now.duration_since(self.t0).as_millis() as u64
                }
            },
        });
    }

    /// Return a snapshot of all recorded entries (oldest first).
    #[must_use]
    pub fn snapshot(&self) -> Vec<TimelineEntry> {
        self.entries.borrow().iter().cloned().collect()
    }

    /// Number of entries currently stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.borrow().len()
    }

    /// Return `true` if the timeline is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.borrow().is_empty()
    }
}
