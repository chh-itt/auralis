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
//! let snap = snapshot();
//! let json = serde_json::to_string_pretty(&snap).unwrap();
//! println!("{json}");
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs, clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod component;
pub mod diff;
mod snapshot;
pub mod stream;
pub mod timeline;

pub use snapshot::{snapshot, DerivationNode, ReactiveSnapshot};
