//! auralis-signal: Reactive signal primitive with version tracking and
//! proactive waker deregistration.
//!
//! # Overview
//!
//! This crate provides [`Signal<T>`] and [`Memo<T>`] — the foundational
//! reactive primitives of the Auralis kernel.  Every signal carries a
//! monotonic version number.  When a change-detection future is dropped
//! before resolution, it eagerly removes its waker from the signal's
//! internal list, preventing stale-waker accumulation (the "proactive
//! deregistration" guarantee).
//!
//! The crate is **zero-dependency**, **`#![forbid(unsafe_code)]`**, and
//! intentionally single-threaded (`!Send` / `!Sync`).  See the
//! [repository design docs](https://github.com/user/auralis) for the
//! rationale behind those choices.
//!
//! # Quick example
//!
//! ```
//! use auralis_signal::Signal;
//!
//! let sig = Signal::new(0);
//! sig.set(1);
//! assert_eq!(sig.read(), 1);
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs, clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod batch;
mod future;
mod memo;
mod observer;
mod signal;

pub use batch::{batch, in_batch};
pub use future::{FilterChangedFuture, MapChangedFuture, SignalChangedFuture};
pub use memo::Memo;
#[doc(hidden)]
pub use signal::{install_schedule_hook, remove_schedule_hook};
#[doc(hidden)]
pub use signal::{subscribe, unsubscribe};
pub use signal::{Signal, SignalMap};
