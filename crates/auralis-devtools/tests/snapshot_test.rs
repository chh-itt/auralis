//! Integration test for `auralis_devtools::snapshot()`.

use auralis_signal::{Memo, Signal};
use auralis_task::TaskScope;

#[test]
fn snapshot_includes_signals() {
    let sig = Signal::new(42);
    sig.set_label("test_sig");
    let snap = auralis_devtools::snapshot();
    let has = snap
        .signals
        .iter()
        .any(|s| s.label.as_deref() == Some("test_sig"));
    assert!(has, "snapshot should include labelled signal");
}

#[test]
fn snapshot_includes_memos_with_dependencies() {
    let a = Signal::new(1);
    let b = Signal::new(2);
    let a2 = a.clone();
    let b2 = b.clone();
    let memo = Memo::new(move || a2.read() + b2.read());
    memo.set_label("sum");

    let snap = auralis_devtools::snapshot();
    let entry = snap
        .memos
        .iter()
        .find(|m| m.label.as_deref() == Some("sum"))
        .expect("snapshot should include labelled memo");

    assert_eq!(entry.dependency_count, 2);
    assert_eq!(entry.dependency_addrs.len(), 2);
    assert!(!entry.dependency_addrs[0].is_empty());
}

#[test]
fn snapshot_includes_task_tree() {
    let _scope = TaskScope::new();
    let snap = auralis_devtools::snapshot();
    assert!(snap.task_tree.contains("Auralis Reactive Graph"));
}

#[test]
fn snapshot_json_serializes() {
    let _sig = Signal::new(0);
    let json = serde_json::to_string_pretty(&auralis_devtools::snapshot()).unwrap();
    assert!(json.contains("\"signals\""));
    assert!(json.contains("\"memos\""));
    assert!(json.contains("\"task_tree\""));
}

#[test]
fn snapshot_diff_detects_changed_signal() {
    let sig = Signal::new(0);
    sig.set_label("test");
    let snap1 = auralis_devtools::snapshot();

    sig.set(42);
    let snap2 = auralis_devtools::snapshot();

    let diff = auralis_devtools::diff::diff_snapshots(&snap1, &snap2);
    assert!(
        !diff.changed_signals.is_empty(),
        "should detect version bump"
    );
    assert_eq!(diff.changed_signals[0].old_version, 0);
    assert_eq!(diff.changed_signals[0].new_version, 1);
}

#[test]
fn snapshot_diff_detects_memo_recompute() {
    let sig = Signal::new(1);
    let s = sig.clone();
    let memo = Memo::new(move || s.read() * 2);
    memo.set_label("double");
    let _ = memo.read();
    let snap1 = auralis_devtools::snapshot();

    sig.set(10);
    let _ = memo.read();
    let snap2 = auralis_devtools::snapshot();

    let diff = auralis_devtools::diff::diff_snapshots(&snap1, &snap2);
    assert!(!diff.changed_memos.is_empty(), "should detect recompute");
    assert_eq!(diff.changed_memos[0].recomputations, 1);
}

#[test]
fn snapshot_diff_empty_when_identical() {
    let _sig = Signal::new(0);
    let snap1 = auralis_devtools::snapshot();
    let snap2 = auralis_devtools::snapshot();

    let diff = auralis_devtools::diff::diff_snapshots(&snap1, &snap2);
    assert!(diff.changed_signals.is_empty());
    assert!(diff.changed_memos.is_empty());
    assert!(diff.new_signals.is_empty());
}

#[test]
fn snapshot_diff_detects_label_change() {
    let sig = Signal::new(0);
    sig.set_label("old_name");
    let snap1 = auralis_devtools::snapshot();

    sig.set_label("new_name");
    let snap2 = auralis_devtools::snapshot();

    let diff = auralis_devtools::diff::diff_snapshots(&snap1, &snap2);
    assert!(
        !diff.changed_signals.is_empty(),
        "label change should be detected"
    );
}

#[test]
fn snapshot_diff_detects_dead_signal() {
    let sig = Signal::new(0);
    sig.set_label("transient");
    let snap1 = auralis_devtools::snapshot();
    drop(sig);
    let snap2 = auralis_devtools::snapshot();

    let diff = auralis_devtools::diff::diff_snapshots(&snap1, &snap2);
    assert_eq!(diff.dead_signals.len(), 1);
    assert!(diff.dead_signals[0].contains("0x"));
}

#[test]
fn timeline_ring_buffer_evicts_old_entries() {
    let tl = auralis_devtools::timeline::Timeline::new(3);
    tl.record(1);
    tl.record(2);
    tl.record(3);
    assert_eq!(tl.len(), 3);

    tl.record(4);
    assert_eq!(tl.len(), 3);
    let snap = tl.snapshot();
    assert_eq!(snap.len(), 3);
    assert_eq!(snap[0].seq, 2); // seq 1 evicted
    assert_eq!(snap[2].seq, 4);
}

#[test]
fn timeline_empty_snapshot() {
    let tl = auralis_devtools::timeline::Timeline::new(10);
    assert!(tl.is_empty());
    assert_eq!(tl.snapshot().len(), 0);
}

#[test]
fn derivation_tree_signals_are_roots() {
    let sig = Signal::new(0);
    sig.set_label("root_sig");
    let snap = auralis_devtools::snapshot();
    assert!(!snap.derivation_tree.is_empty());
    let root = snap
        .derivation_tree
        .iter()
        .find(|n| n.label.as_deref() == Some("root_sig"))
        .expect("root signal must be in derivation tree");
    assert_eq!(root.node_type, "Signal");
    assert!(root.depended_by.is_empty());
}

#[test]
fn derivation_tree_memo_is_child_of_signal() {
    let sig = Signal::new(1);
    sig.set_label("source");
    let s = sig.clone();
    let memo = Memo::new(move || s.read() * 2);
    memo.set_label("double");
    let _ = memo.read();

    let snap = auralis_devtools::snapshot();
    let root = snap
        .derivation_tree
        .iter()
        .find(|n| n.label.as_deref() == Some("source"))
        .expect("source signal must be root");
    let doubled = root
        .depended_by
        .iter()
        .find(|c| c.label.as_deref() == Some("double"))
        .expect("double memo must be child of source");
    assert_eq!(doubled.node_type, "Memo");
    assert!(doubled.depended_by.is_empty());
}

#[test]
fn derivation_tree_memo_with_two_deps_appears_under_both() {
    let a = Signal::new(1);
    let b = Signal::new(10);
    a.set_label("a");
    b.set_label("b");
    let a2 = a.clone();
    let b2 = b.clone();
    let memo = Memo::new(move || a2.read() + b2.read());
    memo.set_label("sum");
    let _ = memo.read();

    let snap = auralis_devtools::snapshot();
    let root_a = snap
        .derivation_tree
        .iter()
        .find(|n| n.label.as_deref() == Some("a"))
        .unwrap();
    let root_b = snap
        .derivation_tree
        .iter()
        .find(|n| n.label.as_deref() == Some("b"))
        .unwrap();
    assert!(
        root_a
            .depended_by
            .iter()
            .any(|c| c.label.as_deref() == Some("sum")),
        "sum memo must appear under both dependencies (a)"
    );
    assert!(
        root_b
            .depended_by
            .iter()
            .any(|c| c.label.as_deref() == Some("sum")),
        "sum memo must appear under both dependencies (b)"
    );
}

#[test]
fn derivation_tree_json_serializes() {
    let _sig = Signal::new(0);
    let json = serde_json::to_string_pretty(&auralis_devtools::snapshot()).unwrap();
    assert!(json.contains("\"derivation_tree\""));
}
