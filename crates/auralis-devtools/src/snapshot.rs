//! JSON-serializable snapshot of the entire reactive graph.

use auralis_signal::{dump_registry, ReactiveNodeSnapshot};
use auralis_task::{dump_reactive_graph, scope_tree, ScopeTreeNode};
use serde::Serialize;

/// A complete, serializable snapshot of the Auralis reactive graph.
#[derive(Debug, Clone, Serialize)]
pub struct ReactiveSnapshot {
    /// All live signals.
    pub signals: Vec<SignalEntry>,
    /// All live memos, each including its dependency addresses.
    pub memos: Vec<MemoEntry>,
    /// Structured scope tree (scopes → tasks), serializable.
    pub scope_tree: Vec<ScopeTreeNode>,
    /// Recent change timeline entries (newest last).  Empty for
    /// standalone `snapshot()` calls; populated by long-lived
    /// servers that track a `Timeline`.
    pub timeline: Vec<crate::timeline::TimelineEntry>,
    /// Optional component tree (set by host frameworks like Leptos).
    pub component_tree: serde_json::Value,
    /// The formatted task tree (text), kept for backward compatibility.
    pub task_tree: String,
}

/// A serializable signal entry.
#[derive(Debug, Clone, Serialize)]
pub struct SignalEntry {
    pub label: Option<String>,
    pub version: u64,
    pub update_count: u64,
    pub subscriber_count: usize,
    pub type_name: String,
    /// Debug representation of the current value (from `set_value_formatter`).
    pub value_debug: Option<String>,
    pub subscribed_by: Vec<String>,
    pub addr: String,
}

/// A serializable memo entry with dependency graph information.
#[derive(Debug, Clone, Serialize)]
pub struct MemoEntry {
    /// Label set via `Memo::set_label()`, if any.
    pub label: Option<String>,
    /// Current version of the memo's output signal.
    pub version: u64,
    /// Number of subscribers watching this memo.
    pub subscriber_count: usize,
    /// Whether the memo has pending recomputation.
    pub is_dirty: bool,
    /// Number of successful recomputations.
    pub compute_count: u64,
    /// Number of source signal dependencies.
    pub dependency_count: usize,
    /// Opaque addresses of the source signals this memo depends on.
    /// Each address matches a `SignalEntry.addr`.
    pub dependency_addrs: Vec<String>,
    /// Microseconds spent in the most recent successful recomputation.
    pub last_compute_us: Option<u64>,
    /// Rust type name of the computed value.
    pub type_name: String,
    /// Opaque identity.
    pub addr: String,
}

fn fmt_addr(addr: usize) -> String {
    format!("{addr:#x}")
}

/// Produce a serializable snapshot of the entire reactive graph.
///
/// Calls `dump_registry()` (from `auralis_signal`'s `diagnostics`
/// feature) and formats the result as [`ReactiveSnapshot`].
///
/// # Panics
///
/// Panics if the signal schedule hook has not been installed (i.e.
/// `auralis_task::init_flush_scheduler` was never called).  Without
/// the hook, signal callbacks execute synchronously and can cause
/// re-entrant borrow panics during the snapshot.
#[must_use]
pub fn snapshot() -> ReactiveSnapshot {
    let nodes: Vec<ReactiveNodeSnapshot> = dump_registry();

    // Build reverse dependency map: signal_addr → [memo_addr]
    let mut subscribed_by: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    let mut signals = Vec::new();
    let mut memos = Vec::new();

    for n in &nodes {
        match n.node_type {
            "Signal" => {
                signals.push(SignalEntry {
                    label: n.label.clone(),
                    version: n.version,
                    update_count: n.update_count.unwrap_or(0),
                    subscriber_count: n.subscriber_count,
                    type_name: n.type_name.clone().unwrap_or_default(),
                    value_debug: n.value_debug.clone(),
                    subscribed_by: Vec::new(),
                    addr: fmt_addr(n.state_addr),
                });
            }
            "Memo" => {
                let dep_addrs: Vec<String> = n
                    .dependency_addrs
                    .as_ref()
                    .map(|addrs| addrs.iter().map(|a| fmt_addr(*a)).collect())
                    .unwrap_or_default();
                let memo_addr = fmt_addr(n.state_addr);
                for dep in &dep_addrs {
                    subscribed_by
                        .entry(dep.clone())
                        .or_default()
                        .push(memo_addr.clone());
                }
                memos.push(MemoEntry {
                    label: n.label.clone(),
                    version: n.version,
                    subscriber_count: n.subscriber_count,
                    is_dirty: n.is_dirty.unwrap_or(false),
                    compute_count: n.compute_count.unwrap_or(0),
                    dependency_count: n.dependency_count.unwrap_or(0),
                    dependency_addrs: dep_addrs,
                    last_compute_us: n.last_compute_us,
                    type_name: n.type_name.clone().unwrap_or_default(),
                    addr: memo_addr,
                });
            }
            _ => {}
        }
    }

    // Fill subscribed_by on each signal.
    for s in &mut signals {
        if let Some(subbers) = subscribed_by.get(&s.addr) {
            s.subscribed_by = subbers.clone();
        }
    }

    ReactiveSnapshot {
        signals,
        memos,
        scope_tree: scope_tree(),
        timeline: Vec::new(),
        component_tree: serde_json::to_value(crate::component::component_tree())
            .unwrap_or(serde_json::Value::Null),
        task_tree: dump_reactive_graph(),
    }
}
