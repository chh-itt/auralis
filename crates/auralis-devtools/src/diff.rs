//! Snapshot diff — compare two [`ReactiveSnapshot`]s and report
//! what changed between them.
//!
//! Useful for debugging "what happened between this set and that
//! set" or finding unexpected recomputation cascades.

use std::collections::HashMap;

use serde::Serialize;

use crate::snapshot::{MemoEntry, ReactiveSnapshot, SignalEntry};

/// The result of comparing two snapshots.
///
/// Nodes are matched by `addr`.  A signal or memo that appears in
/// `prev` but not `current` is *dead*; one that appears in `current`
/// but not `prev` is *new*; one that appears in both with a version
/// bump is *changed*.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotDiff {
    /// Signals that exist only in the current snapshot.
    pub new_signals: Vec<SignalEntry>,
    /// Addresses of signals that existed only in the previous snapshot.
    pub dead_signals: Vec<String>,
    /// Signals with version bumps (or subscriber-count changes).
    pub changed_signals: Vec<SignalChange>,
    /// Memos that exist only in the current snapshot.
    pub new_memos: Vec<MemoEntry>,
    /// Addresses of memos that existed only in the previous snapshot.
    pub dead_memos: Vec<String>,
    /// Memos that recomputed or changed dirty state.
    pub changed_memos: Vec<MemoChange>,

    /// Wall-clock duration between the two snapshots, if known.
    /// Set by the caller after constructing the diff.
    pub elapsed_ms: Option<u64>,
}

/// A signal that changed between two snapshots.
#[derive(Debug, Clone, Serialize)]
pub struct SignalChange {
    /// Opaque address.
    pub addr: String,
    /// Label, if set.
    pub label: Option<String>,
    /// Version in the previous snapshot.
    pub old_version: u64,
    /// Version in the current snapshot.
    pub new_version: u64,
    /// Change in subscriber count (current - previous).
    pub subscriber_delta: i64,
}

/// A memo that changed between two snapshots.
#[derive(Debug, Clone, Serialize)]
pub struct MemoChange {
    /// Opaque address.
    pub addr: String,
    /// Label, if set.
    pub label: Option<String>,
    /// Compute count in the previous snapshot.
    pub old_compute_count: u64,
    /// Compute count in the current snapshot.
    pub new_compute_count: u64,
    /// Number of recomputations between the two snapshots.
    pub recomputations: u64,
    /// Whether the memo was dirty in the old snapshot.
    pub was_dirty: bool,
    /// Whether the memo is dirty now.
    pub is_dirty: bool,
    /// Change in subscriber count.
    pub subscriber_delta: i64,
    /// Change in dependency count.
    pub dependency_delta: i64,
}

/// Compare two snapshots and return a [`SnapshotDiff`].
///
/// # Example
///
/// ```rust,ignore
/// let snap1 = auralis_devtools::snapshot();
/// // ... mutate some signals ...
/// let snap2 = auralis_devtools::snapshot();
/// let diff = auralis_devtools::diff::diff_snapshots(&snap1, &snap2);
/// println!("{} memos recomputed", diff.changed_memos.len());
/// ```
#[must_use]
#[allow(clippy::cast_possible_wrap)]
pub fn diff_snapshots(prev: &ReactiveSnapshot, current: &ReactiveSnapshot) -> SnapshotDiff {
    // Index previous state by addr.
    let prev_sigs: HashMap<&str, &SignalEntry> =
        prev.signals.iter().map(|s| (s.addr.as_str(), s)).collect();
    let prev_memos: HashMap<&str, &MemoEntry> =
        prev.memos.iter().map(|m| (m.addr.as_str(), m)).collect();

    let mut new_signals = Vec::new();
    let mut dead_signals: Vec<String> = prev_sigs.keys().map(|a| (*a).to_string()).collect();
    let mut changed_signals = Vec::new();
    let mut new_memos = Vec::new();
    let mut dead_memos: Vec<String> = prev_memos.keys().map(|a| (*a).to_string()).collect();
    let mut changed_memos = Vec::new();

    for s in &current.signals {
        if let Some(old) = prev_sigs.get(s.addr.as_str()) {
            dead_signals.retain(|a| a != &s.addr);
            if s.version != old.version
                || s.subscriber_count != old.subscriber_count
                || s.label != old.label
            {
                changed_signals.push(SignalChange {
                    addr: s.addr.clone(),
                    label: s.label.clone(),
                    old_version: old.version,
                    new_version: s.version,
                    subscriber_delta: s.subscriber_count as i64 - old.subscriber_count as i64,
                });
            }
        } else {
            new_signals.push(s.clone());
        }
    }

    for m in &current.memos {
        if let Some(old) = prev_memos.get(m.addr.as_str()) {
            dead_memos.retain(|a| a != &m.addr);
            if m.compute_count != old.compute_count
                || m.is_dirty != old.is_dirty
                || m.label != old.label
            {
                changed_memos.push(MemoChange {
                    addr: m.addr.clone(),
                    label: m.label.clone(),
                    old_compute_count: old.compute_count,
                    new_compute_count: m.compute_count,
                    recomputations: m.compute_count.saturating_sub(old.compute_count),
                    was_dirty: old.is_dirty,
                    is_dirty: m.is_dirty,
                    subscriber_delta: m.subscriber_count as i64 - old.subscriber_count as i64,
                    dependency_delta: m.dependency_count as i64 - old.dependency_count as i64,
                });
            }
        } else {
            new_memos.push(m.clone());
        }
    }

    SnapshotDiff {
        new_signals,
        dead_signals,
        changed_signals,
        new_memos,
        dead_memos,
        changed_memos,
        elapsed_ms: None,
    }
}
