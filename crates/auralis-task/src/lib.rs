//! auralis-task: Scoped async task runtime with explicit [`TaskScope`]
//! hierarchy, iterative cancellation, and priority scheduling.
//!
//! # Architecture
//!
//! - **Global executor** — a [`thread_local!`] singleton with
//!   high- and low-priority queues.
//! - **`TaskScope`** — owns spawned futures; dropping a scope cancels
//!   all descendant scopes and their tasks **iteratively** (no
//!   recursion), preventing stack overflows in deeply nested UI trees.
//! - **`set_deferred`** — safe signal mutation from [`Drop`] contexts.
//! - **Pluggable storage** — [`ScopeStore`] and [`Executor::new_instance`]
//!   enable multi-request isolation for SSR or multi-threaded runtimes.
//! - **Diagnostics** — `dump_reactive_graph()` and [`scope_tree`]
//!   (behind the `debug` feature) provide structured introspection
//!   of the reactive graph.  [`TaskScope`] supports optional labels
//!   for diagnostic output.
//!
//! # Quick example
//!
//! ```
//! use auralis_task::{TaskScope, spawn_global};
//!
//! let scope = TaskScope::new();
//! scope.spawn(async { /* ... */ });
//! ```
//!
//! # Design decisions
//!
//! The crate shares `auralis_signal`'s single-threaded-by-default
//! philosophy (`Rc` over `Arc`, `RefCell` over `Mutex`).  For
//! multi-threaded use-cases, create an isolated [`Executor`] instance
//! per thread or per request.  See the repository design docs for the
//! full rationale.

#![forbid(unsafe_code)]
#![warn(missing_docs, clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod executor;
pub mod scheduler;
mod scope;
pub mod timer;

#[cfg(feature = "debug")]
mod debug;

#[cfg(feature = "debug")]
pub use debug::{dump_reactive_graph, dump_task_tree};
pub use executor::{
    drain_deferred_signal_callbacks, has_flush_scheduler, init_flush_scheduler, init_time_source,
    remove_panic_hook, reset_executor_for_test, schedule_callback, set_deferred,
    set_global_max_deferred_callbacks, set_global_time_budget, set_panic_hook, spawn_global,
    spawn_global_with_priority, with_executor, yield_now, Executor, PanicInfo, ScheduleFlush,
    TimeSource, YieldNow,
};
#[cfg(test)]
pub use executor::{TestScheduleFlush, TestTimeSource};
pub use scheduler::DeferredScheduler;
pub use scope::{
    clear_scope_registry, current_scope, find_scope, set_scope_store, with_current_scope,
    CallbackHandle, JoinHandle, ScopeStore, TaskScope,
};
#[cfg(feature = "debug")]
pub use scope::{scope_debug_label, scope_tree, ScopeTreeNode, TaskNode};

#[cfg(feature = "ssr-tokio")]
pub use scope::init_scope_store_tokio;

/// Task priority.
///
/// High-priority tasks are dequeued before low-priority ones within a
/// single flush cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// Dequeued first during flush.
    High,
    /// Dequeued after all high-priority tasks.
    Low,
}
