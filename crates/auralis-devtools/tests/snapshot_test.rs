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
