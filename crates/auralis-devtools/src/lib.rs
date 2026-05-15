//! auralis-devtools: Diagnostic `DevTools` for the Auralis reactive kernel.
//!
//! # Overview
//!
//! This crate provides:
//!
//! - **JSON snapshots** — call [`snapshot`] to capture every live signal,
//!   memo, and task as a serializable structure.
//! - **Real-time change stream** — [`change_stream`] wraps an observer
//!   hook and delivers batched change events.
//! - **CLI** — `auralis-devtools dump` prints a snapshot to stdout;
//!   `auralis-devtools serve` starts a WebSocket real-time feed.
//!
//! # Quick example
//!
//! ```rust,ignore
//! use auralis_devtools::snapshot;
//!
//! let json = snapshot();
//! println!("{json}");
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs, clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod snapshot;
pub mod stream;

pub use snapshot::{snapshot, ReactiveSnapshot};
