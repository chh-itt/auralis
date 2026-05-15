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
//! Signals and memos support optional **labels** for diagnostic output,
//! and the [`add_schedule_observer`] hook lets external tools observe
//! every signal mutation.  Behind the `diagnostics` feature (enabled by
//! `auralis_task`'s `debug` feature), a thread-local registry tracks
//! live nodes for `dump_registry()`.
//!
//! The crate is **zero-dependency**, **`#![forbid(unsafe_code)]`**, and
//! intentionally single-threaded (`!Send` / `!Sync`).  See the
//! [repository design docs](https://github.com/chh-itt/auralis) for the
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
#[cfg(feature = "diagnostics")]
mod registry;
#[cfg(feature = "diagnostics")]
pub use registry::{dump_registry, ReactiveNodeSnapshot};
mod signal;

/// Create a [`Memo`] with automatic clone of captured identifiers.
///
/// Instead of manually cloning every signal before the `move` closure:
///
/// ```
/// use auralis_signal::{Signal, Memo};
///
/// let a = Signal::new(1);
/// let b = Signal::new(2);
///
/// // Without the macro:
/// let sum = Memo::new({
///     let a = a.clone();
///     let b = b.clone();
///     move || a.read() + b.read()
/// });
///
/// // With the macro:
/// let sum = auralis_signal::memo!(a, b => a.read() + b.read());
/// ```
///
/// The macro expands to the same pattern — a `move` closure with each
/// captured identifier cloned into a local variable of the same name.
#[macro_export]
macro_rules! memo {
    ($($capture:ident),+ $(,)? => $body:expr) => {
        $crate::Memo::new({
            $(let $capture = $capture.clone();)+
            move || $body
        })
    };
}

pub use batch::{batch, in_batch};
pub use future::{FilterChangedFuture, MapChangedFuture, SignalChangedFuture};
pub use memo::Memo;
pub use signal::{
    add_schedule_observer, add_schedule_observer_with_identity, install_timing_hook, now_us,
    remove_schedule_observer, ObserverToken,
};
#[doc(hidden)]
pub use signal::{install_schedule_hook, remove_schedule_hook};
#[doc(hidden)]
pub use signal::{subscribe, unsubscribe};
pub use signal::{Signal, SignalMap};
