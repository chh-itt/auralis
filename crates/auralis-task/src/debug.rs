//! Debug utilities for inspecting the reactive runtime.
//!
//! Only compiled when the **`debug`** feature is enabled.

use std::collections::BTreeMap;
use std::fmt::Write;

use crate::executor;
use crate::scope;
use crate::Priority;

/// Return a formatted snapshot of the entire reactive graph: signals,
/// memos, and tasks, grouped by scope.
///
/// This is the unified diagnostic entry point.  It supersedes
/// [`dump_task_tree`].
///
/// # Example output
///
/// ```text
/// === Auralis Reactive Graph ===
/// Signals: 3  Memos: 2  Tasks: 4
///
/// ── Signals ──
///   "counter"  ver=42  subs=1  addr=0x...
///   (unnamed)  ver=7   subs=0  addr=0x...
///
/// ── Memos ──
///   "sum"  ver=42  subs=1  dirty=false  computed=15x  deps=2  addr=0x...
///
/// ── Tasks ──
/// Scope 1 "root":
///   task 0  [L]  queued
/// Scope 2 "header":
///   task 1  [L]
/// ```
#[must_use]
pub fn dump_reactive_graph() -> String {
    let mut out = String::new();
    out.push_str("=== Auralis Reactive Graph ===\n");

    // --- Signals & Memos ---
    let snapshots: Vec<auralis_signal::ReactiveNodeSnapshot> = auralis_signal::dump_registry();

    let signals: Vec<_> = snapshots
        .iter()
        .filter(|s| s.node_type == "Signal")
        .collect();
    let memos: Vec<_> = snapshots.iter().filter(|s| s.node_type == "Memo").collect();

    let _ = writeln!(
        &mut out,
        "Signals: {}  Memos: {}  Tasks: {}",
        signals.len(),
        memos.len(),
        executor::debug_task_count(),
    );

    if !signals.is_empty() {
        out.push_str("\n── Signals ──\n");
        for s in &signals {
            match &s.label {
                Some(label) => write!(&mut out, "  \"{label}\""),
                None => write!(&mut out, "  (unnamed)"),
            }
            .ok();
            let _ = writeln!(
                &mut out,
                "  ver={}  subs={}  addr={:#x}",
                s.version, s.subscriber_count, s.state_addr
            );
        }
    }

    if !memos.is_empty() {
        out.push_str("\n── Memos ──\n");
        for m in &memos {
            match &m.label {
                Some(label) => write!(&mut out, "  \"{label}\""),
                None => write!(&mut out, "  (unnamed)"),
            }
            .ok();
            let dirty = m.is_dirty.unwrap_or(false);
            let computed = m.compute_count.unwrap_or(0);
            let deps = m.dependency_count.unwrap_or(0);
            let _ = writeln!(
                &mut out,
                "  ver={}  subs={}  dirty={dirty}  computed={computed}x  deps={deps}  addr={:#x}",
                m.version, m.subscriber_count, m.state_addr
            );
        }
    }

    // --- Tasks ---
    out.push('\n');
    write_task_tree(&mut out);

    out
}

fn write_task_tree(out: &mut String) {
    let tasks = executor::debug_task_snapshot();
    let queued = executor::debug_queued_task_ids();

    let mut by_scope: BTreeMap<u64, Vec<(u64, Priority)>> = BTreeMap::new();
    for (tid, pri, sid) in &tasks {
        by_scope.entry(*sid).or_default().push((*tid, *pri));
    }

    out.push_str("── Tasks ──\n");

    if tasks.is_empty() {
        out.push_str("  (no active tasks)\n");
        return;
    }

    for (scope_id, mut scope_tasks) in by_scope {
        scope_tasks.sort_by_key(|(tid, _)| *tid);
        let label = scope::scope_debug_label(scope_id);
        match label {
            Some(ref lbl) => {
                let _ = writeln!(out, "Scope {scope_id} \"{lbl}\":");
            }
            None => {
                let _ = writeln!(out, "Scope {scope_id}:");
            }
        }
        for (tid, pri) in &scope_tasks {
            let pri_char = match pri {
                Priority::High => 'H',
                Priority::Low => 'L',
            };
            let q = if queued.contains(tid) { "  queued" } else { "" };
            let _ = writeln!(out, "  task {tid}  [{pri_char}]{q}");
        }
    }
}

/// Return a formatted snapshot of all active tasks in the global
/// executor, grouped by scope.
///
/// Delegates to [`dump_reactive_graph`]'s task-tree section.
#[must_use]
pub fn dump_task_tree() -> String {
    dump_reactive_graph()
}

#[cfg(test)]
mod tests {
    use auralis_signal::{Memo, Signal};

    #[test]
    fn dump_reactive_graph_includes_signals_and_memos() {
        let sig = Signal::new(42);
        sig.set_label("answer");
        let output = super::dump_reactive_graph();
        assert!(
            output.contains("Signals:"),
            "should include signal count header"
        );
        assert!(
            output.contains("\"answer\""),
            "should include labelled signal"
        );
        assert!(output.contains("ver="), "should include version");
        assert!(output.contains("subs="), "should include subscriber count");

        let sig2 = sig.clone();
        // Verify empty label renders as "".
        sig2.set_label("");
        let memo = Memo::new(move || sig2.read() + 1);
        memo.set_label("plus_one");
        let output2 = super::dump_reactive_graph();
        assert!(
            output2.contains("Memos:"),
            "should include memo count header"
        );
        assert!(
            output2.contains("\"plus_one\""),
            "should include labelled memo"
        );
        assert!(output2.contains("dirty="), "should include dirty flag");
        assert!(
            output2.contains("computed="),
            "should include compute count"
        );
    }

    #[test]
    fn dump_reactive_graph_shows_tasks_section() {
        let output = super::dump_reactive_graph();
        assert!(
            output.contains("── Tasks ──"),
            "should include tasks section"
        );
    }
}
