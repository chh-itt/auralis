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
    /// Derivation tree: signals → memos, organised by data-dependency
    /// edges.  Roots are original signals; branches are derived memos.
    pub derivation_tree: Vec<DerivationNode>,
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

/// A node in the derivation tree — the reactive data-flow graph
/// rendered as a tree.  Roots are original signals (no dependencies);
/// branches are memos that depend on them.
///
/// A memo with *N* source dependencies appears as a child under *all
/// N* parents.  The DAG is expanded into a tree by duplicating
/// multi-parent nodes.
#[derive(Debug, Clone, Serialize)]
pub struct DerivationNode {
    /// Label set via `set_label()`, if any.
    pub label: Option<String>,
    /// Hex address of the underlying `SignalState` allocation.
    pub addr: String,
    /// `"Signal"` or `"Memo"`.
    pub node_type: String,
    /// Rust type name.
    pub type_name: Option<String>,
    /// Debug representation of the current value, if a formatter was set.
    pub value_debug: Option<String>,
    /// Current version.
    pub version: u64,
    /// Nodes that depend on this one (children in the tree).
    pub depended_by: Vec<DerivationNode>,
}

fn fmt_addr(addr: usize) -> String {
    format!("{addr:#x}")
}

/// Build a derivation tree from the flat signal/memo lists.
///
/// Roots are nodes with no known dependencies (original signals, and
/// any memo whose sources have all been dropped).  Multi-parent memos
/// appear as children under *each* parent — the DAG is expanded into a
/// tree by duplication.
fn build_derivation_tree(signals: &[SignalEntry], memos: &[MemoEntry]) -> Vec<DerivationNode> {
    use std::collections::{HashMap, HashSet};

    // addr → (label, node_type, type_name, value_debug, version)
    struct NodeMeta {
        label: Option<String>,
        node_type: String,
        type_name: Option<String>,
        value_debug: Option<String>,
        version: u64,
    }

    fn build_recursive(
        addr: &str,
        meta: &HashMap<String, NodeMeta>,
        children: &HashMap<String, Vec<String>>,
        visited: &mut HashSet<String>,
    ) -> DerivationNode {
        let info = meta.get(addr).expect("node metadata must exist");
        let kid_addrs: Vec<String> = children.get(addr).cloned().unwrap_or_default();

        let mut depended_by: Vec<DerivationNode> = Vec::new();
        for kid in &kid_addrs {
            if visited.insert(kid.clone()) {
                depended_by.push(build_recursive(kid, meta, children, visited));
                visited.remove(kid);
            }
        }

        DerivationNode {
            label: info.label.clone(),
            addr: addr.to_string(),
            node_type: info.node_type.clone(),
            type_name: info.type_name.clone(),
            value_debug: info.value_debug.clone(),
            version: info.version,
            depended_by,
        }
    }

    let mut node_meta: HashMap<String, NodeMeta> = HashMap::new();
    let mut roots: Vec<String> = Vec::new();

    for s in signals {
        node_meta.insert(
            s.addr.clone(),
            NodeMeta {
                label: s.label.clone(),
                node_type: "Signal".into(),
                type_name: Some(s.type_name.clone()),
                value_debug: s.value_debug.clone(),
                version: s.version,
            },
        );
        roots.push(s.addr.clone());
    }

    // children[dep_addr] = [child_addr]
    let mut children: HashMap<String, Vec<String>> = HashMap::new();

    for m in memos {
        node_meta.insert(
            m.addr.clone(),
            NodeMeta {
                label: m.label.clone(),
                node_type: "Memo".into(),
                type_name: Some(m.type_name.clone()),
                value_debug: None,
                version: m.version,
            },
        );

        let known_deps: Vec<&str> = m
            .dependency_addrs
            .iter()
            .filter(|a| node_meta.contains_key(*a))
            .map(String::as_str)
            .collect();

        if known_deps.is_empty() {
            roots.push(m.addr.clone());
        } else {
            for dep in known_deps {
                children
                    .entry(dep.to_string())
                    .or_default()
                    .push(m.addr.clone());
            }
        }
    }

    let mut visited: HashSet<String> = HashSet::new();
    let mut tree = Vec::new();
    for root in &roots {
        if visited.insert(root.clone()) {
            tree.push(build_recursive(root, &node_meta, &children, &mut visited));
            visited.remove(root);
        }
    }
    tree
}

/// Produce a serializable snapshot of the entire reactive graph.
///
/// Calls `dump_registry()` (from `auralis_signal`'s `diagnostics`
/// feature) and formats the result as [`ReactiveSnapshot`].
///
/// # Auto-initialisation
///
/// On first call this automatically invokes [`crate::init`], which
/// installs a built-in flush scheduler if the user hasn't already.
/// Pending signal notifications are drained before the snapshot is
/// taken, so memo and subscriber state is consistent.
#[must_use]
pub fn snapshot() -> ReactiveSnapshot {
    crate::init();
    crate::drain_auto_scheduler();

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

    let derivation_tree = build_derivation_tree(&signals, &memos);

    ReactiveSnapshot {
        signals,
        memos,
        scope_tree: scope_tree(),
        timeline: Vec::new(),
        component_tree: serde_json::to_value(crate::component::component_tree())
            .unwrap_or(serde_json::Value::Null),
        derivation_tree,
        task_tree: dump_reactive_graph(),
    }
}
