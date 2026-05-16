//! auralis-devtools: Diagnostic `DevTools` for the Auralis reactive kernel.
//!
//! # Overview
//!
//! - [`snapshot`] — capture every live signal, memo, and task as a
//!   serializable [`ReactiveSnapshot`].
//! - [`diff::diff_snapshots`] — compare two snapshots to see what changed.
//! - [`stream::change_stream`] — real-time change events via an observer hook.
//! - [`timeline::Timeline`] — ring buffer of recent mutation timestamps.
//! - **CLI** — `auralis-devtools dump` prints a JSON snapshot to stdout.
//!
//! # Quick example
//!
//! ```rust,ignore
//! use auralis_devtools::snapshot;
//!
//! // init() is optional — snapshot() calls it automatically on first use.
//! // Explicit init() is only needed if you want to control timing.
//! let snap = snapshot();
//! let json = serde_json::to_string_pretty(&snap).unwrap();
//! println!("{json}");
//! ```
//!
//! # Zero-setup diagnostics
//!
//! Calling [`init`] installs a built-in [`DeferredScheduler`] (if the user
//! hasn't already installed one via [`auralis_task::init_flush_scheduler`]).
//! This means **no boilerplate** — just `cargo add auralis-devtools` and
//! you're ready to call [`snapshot`].
//!
//! If your app already uses [`auralis_task::TaskScope`] with its own
//! scheduler, [`init`] detects it and becomes a no-op.  Your existing
//! drain loop keeps everything working; [`snapshot`] just reads the
//! registries.

#![forbid(unsafe_code)]
#![warn(missing_docs, clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

use std::cell::RefCell;
use std::rc::Rc;

pub mod component;
pub mod diff;
mod snapshot;
pub mod stream;
pub mod timeline;

#[cfg(feature = "wasm-bridge")]
pub mod wasm;

pub use snapshot::{snapshot, DerivationNode, ReactiveSnapshot};

// ---------------------------------------------------------------------------
// Auto-init: install a DeferredScheduler if the user hasn't already.
// ---------------------------------------------------------------------------

thread_local! {
    /// Holds the devtools-installed [`DeferredScheduler`], if any.
    /// `snapshot()` drains this before reading the registries.
    static AUTO_SCHEDULER: RefCell<Option<Rc<auralis_task::scheduler::DeferredScheduler>>> =
        const { RefCell::new(None) };
}

/// Initialize the `DevTools` runtime.
///
/// If the user hasn't already installed a flush scheduler via
/// [`auralis_task::init_flush_scheduler`], this installs a built-in
/// [`DeferredScheduler`](auralis_task::scheduler::DeferredScheduler)
/// so that signal notifications flow correctly and [`snapshot`]
/// returns consistent results.
///
/// # Idempotency
///
/// Safe to call multiple times.  If a scheduler is already installed
/// (by the user or by a prior call to this function), subsequent calls
/// are no-ops.
///
/// # Coexistence with user schedulers
///
/// If the user has already installed their own scheduler (e.g. for
/// [`auralis_task::TaskScope`]), this function is a **no-op**.  The
/// user's drain loop handles notification processing.
///
/// # When to call explicitly
///
/// You only need to call this if you want to control *when* the
/// scheduler is installed.  Otherwise, [`snapshot`] calls it
/// automatically on first use.
pub fn init() {
    if auralis_task::has_flush_scheduler() {
        return;
    }

    let sched = auralis_task::scheduler::DeferredScheduler::new();
    auralis_task::init_flush_scheduler(sched.clone());
    AUTO_SCHEDULER.with(|cell| {
        *cell.borrow_mut() = Some(sched);
    });
}

/// Drain the devtools-installed auto-scheduler, if any, and flush
/// any deferred callbacks that accumulated on the executor before a
/// scheduler was installed.
///
/// Called by [`snapshot`] before reading the registries so that
/// pending signal notifications are processed and memo state is
/// consistent.
pub(crate) fn drain_auto_scheduler() {
    // First, drain callbacks that accumulated in the executor's
    // deferred_callbacks before a scheduler was installed.  These
    // are NOT covered by DeferredScheduler::drain().
    auralis_task::drain_deferred_signal_callbacks();

    // Then drain through the scheduler (if we installed one) to
    // trigger flush_instance, which processes timer expirations and
    // polls tasks.
    AUTO_SCHEDULER.with(|cell| {
        if let Some(sched) = cell.borrow().as_ref() {
            sched.drain();
        }
    });
}
