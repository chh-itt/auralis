//! Debug utilities for inspecting the task runtime.
//!
//! Only compiled when the **`debug`** feature is enabled.

use std::fmt::Write;

use crate::executor;
use crate::scope;
use crate::Priority;

/// Return a formatted snapshot of all active tasks in the global
/// executor, grouped by scope.
///
/// Each line shows a task's id, priority (`H`/`L`), owning scope, and
/// whether it is currently enqueued (ready to poll).  If a scope has
/// a debug label set via [`TaskScope::set_debug_label`], it is shown
/// alongside the scope id.
///
/// # Example output
///
/// ```text
/// === Auralis Task Tree ===
/// Total active tasks: 3
///
/// Scope 1:
///   task 0  [L]  queued
///   task 2  [H]
/// Scope 2 "header":
///   task 1  [L]  queued
/// ```
#[must_use]
pub fn dump_task_tree() -> String {
    let tasks = executor::debug_task_snapshot();
    let queued = executor::debug_queued_task_ids();

    // Group tasks by scope_id.
    let mut by_scope: std::collections::BTreeMap<u64, Vec<(u64, Priority)>> =
        std::collections::BTreeMap::new();
    for (tid, pri, sid) in &tasks {
        by_scope.entry(*sid).or_default().push((*tid, *pri));
    }

    let mut out = String::new();
    out.push_str("=== Auralis Task Tree ===\n");
    let _ = writeln!(&mut out, "Total active tasks: {}\n", tasks.len());

    if tasks.is_empty() {
        out.push_str("(no active tasks)\n");
        return out;
    }

    for (scope_id, mut scope_tasks) in by_scope {
        scope_tasks.sort_by_key(|(tid, _)| *tid);
        // Try to look up a debug label for this scope.
        let label = scope::scope_debug_label(scope_id);
        match label {
            Some(ref lbl) => {
                let _ = writeln!(&mut out, "Scope {scope_id} \"{lbl}\":");
            }
            None => {
                let _ = writeln!(&mut out, "Scope {scope_id}:");
            }
        }
        for (tid, pri) in &scope_tasks {
            let pri_char = match pri {
                Priority::High => 'H',
                Priority::Low => 'L',
            };
            let q = if queued.contains(tid) { "  queued" } else { "" };
            let _ = writeln!(&mut out, "  task {tid}  [{pri_char}]{q}");
        }
    }

    out
}
