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
