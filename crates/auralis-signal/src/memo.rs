//! `Memo<T>` — a computed signal that automatically tracks its
//! dependencies via the observer mechanism in [`super::observer`].
//!
//! # How it works
//!
//! 1.  `Memo::new(compute)` installs an [`ObserverState`] and runs
//!     `compute` **exactly once**.  During that single call, every
//!     [`Signal::read`] / [`Signal::with`] inside `compute`
//!     auto-subscribes the memo to that source signal, and the
//!     returned value becomes both the initial value and the
//!     dependency snapshot.
//! 2.  When any source signal changes, the memo is marked *dirty* (a
//!     cheap flag flip).  No computation happens yet — the memo is
//!     **lazy**.
//! 3.  `Memo::read()` / `Memo::with()` checks the dirty flag.  If
//!     dirty, it re-runs `compute` (which incrementally updates
//!     subscriptions — only unsubscribing from removed dependencies
//!     and subscribing to new ones).  The internal [`Signal`] is
//!     updated and dirty is cleared.
//! 4.  `Memo::drop()` runs all stored cleanup closures, unsubscribing
//!     from every source signal.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::fmt;
use std::rc::Rc;

use crate::observer::{ObserverState, OBSERVER};
use crate::signal::Signal;

pub(crate) type CleanupFn = Box<dyn FnOnce()>;

thread_local! {
    /// Tracks the depth of nested `Memo::recompute` calls on this thread.
    /// Used to detect circular Memo dependencies (two memos reading each
    /// other would recurse indefinitely without this guard).
    #[allow(clippy::missing_const_for_thread_local)]
    static RECOMPUTE_DEPTH: Cell<u32> = Cell::new(0);
}

struct DepthGuard;

impl DepthGuard {
    fn enter() -> Self {
        RECOMPUTE_DEPTH.with(|d| {
            let v = d.get() + 1;
            d.set(v);
            if v > 256 {
                d.set(0);
                panic!(
                    "circular Memo dependency detected (recompute depth > 256). \
                     Check for memos that read each other, directly or indirectly."
                );
            }
        });
        DepthGuard
    }
}

impl Drop for DepthGuard {
    fn drop(&mut self) {
        RECOMPUTE_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}
/// Each entry pairs a [`SignalKey`] with its unsubscribe closure.
/// The key enables incremental diff during recomputation: shared
/// dependencies are kept, only removed/new ones are updated.
type SubscriptionList = Rc<RefCell<Vec<(SignalKey, CleanupFn)>>>;

/// Opaque key for deduplicating observer subscriptions.
///
/// Two signals are considered "the same" if their inner `Rc` points to
/// the same allocation.  The address is stored as `usize` rather than a
/// raw pointer to make the opaque-identifier intent obvious — this is
/// safe because `Signal<T>` is `!Send + !Sync`, so the `Rc` allocation
/// never moves to another thread and its address is a stable identity.
#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub(crate) struct SignalKey {
    addr: usize,
}

impl SignalKey {
    pub(crate) fn new<T>(sig: &Signal<T>) -> Self {
        Self {
            addr: sig.state_addr(),
        }
    }

    pub(crate) fn addr(self) -> usize {
        self.addr
    }
}

/// A lazy, auto-tracking computed signal.
///
/// `Memo<T>` is the Auralis equivalent of `SolidJS`'s `createMemo` or
/// Leptos's `Memo<T>`.  It reads from one or more source [`Signal`]s
/// and recomputes its value only when those sources change **and**
/// someone calls [`read`](Memo::read) or [`with`](Memo::with).
///
/// # Example
///
/// ```
/// use auralis_signal::{Signal, Memo};
///
/// let a = Signal::new(2);
/// let b = Signal::new(3);
/// let a2 = a.clone();
/// let b2 = b.clone();
/// let sum = Memo::new(move || a2.read() + b2.read());
///
/// assert_eq!(sum.read(), 5);
/// a.set(10);
/// assert_eq!(sum.read(), 13); // lazily recomputed
/// ```
pub struct Memo<T> {
    /// Internal signal that stores the computed value and version.
    signal: Signal<T>,
    /// `true` when at least one source has changed since the last
    /// recomputation.
    dirty: Rc<Cell<bool>>,
    /// The user-provided compute function.
    compute: Rc<dyn Fn() -> T>,
    /// Cleanup closures for the currently-active source subscriptions.
    /// Replaced on every successful recomputation; kept intact if
    /// compute panics (so the memo stays connected to its sources).
    subscriptions: SubscriptionList,
    /// `true` while recompute is in progress.  The dirty callback
    /// suppresses `bump_version` when this is set, preventing
    /// re-entrant reader wake-ups during compute.
    computing: Rc<Cell<bool>>,
    /// Number of successful recomputations (including the initial
    /// compute in [`new`](Memo::new)).
    compute_count: Rc<Cell<u64>>,
    /// Optional label set via [`set_label`](Memo::set_label).
    label: Rc<RefCell<Option<String>>>,
    /// Microseconds spent in the most recent successful recomputation.
    last_compute_us: Rc<Cell<u64>>,
}

impl<T: Clone + 'static> Memo<T> {
    /// Create a new memo from a compute function.
    ///
    /// The function is called **exactly once** during construction.
    /// An observer is installed before the call so that dependency
    /// tracking and initial-value evaluation happen in a single pass.
    ///
    /// # Panics
    ///
    /// Panics if the compute function panics.
    #[must_use]
    pub fn new(compute: impl Fn() -> T + 'static) -> Self {
        let compute: Rc<dyn Fn() -> T> = Rc::new(compute);
        let dirty = Rc::new(Cell::new(true));
        let subscriptions: SubscriptionList = Rc::new(RefCell::new(Vec::new()));
        let computing = Rc::new(Cell::new(false));
        let compute_count = Rc::new(Cell::new(0));

        let holder: Rc<RefCell<Option<Signal<T>>>> = Rc::new(RefCell::new(None));

        let (value, _, _) = run_compute(
            &compute,
            &dirty,
            &subscriptions,
            &holder,
            &computing,
            &HashSet::new(),
        );

        let signal = Signal::new_untracked(value);
        *holder.borrow_mut() = Some(signal.clone());

        let memo = Self {
            signal,
            dirty,
            compute,
            subscriptions,
            computing,
            compute_count,
            label: Rc::new(RefCell::new(None)),
            last_compute_us: Rc::new(Cell::new(0)),
        };

        memo.dirty.set(false);
        memo.compute_count.set(1);

        #[cfg(feature = "diagnostics")]
        {
            let weak_subs = Rc::downgrade(&memo.subscriptions);
            let weak_signal = Rc::downgrade(&memo.signal.state);
            let state_addr = Rc::as_ptr(&memo.signal.state) as usize;
            crate::registry::register(crate::registry::make_memo_callback(
                weak_subs,
                weak_signal,
                Rc::clone(&memo.dirty),
                Rc::clone(&memo.compute_count),
                Rc::clone(&memo.label),
                Rc::clone(&memo.last_compute_us),
                state_addr,
            ));
        }

        memo
    }

    /// Return a clone of the current value.
    ///
    /// If any source signal has changed since the last read, the compute
    /// function is re-run before returning.
    #[must_use]
    pub fn read(&self) -> T {
        if self.dirty.get() {
            self.recompute();
        }
        self.signal.read()
    }

    /// Borrow the current value immutably.
    ///
    /// Like [`read`](Memo::read), this recomputes if dirty.
    #[must_use]
    pub fn with<U>(&self, f: impl FnOnce(&T) -> U) -> U {
        if self.dirty.get() {
            self.recompute();
        }
        self.signal.with(f)
    }

    /// Return a future that resolves with the current value after the
    /// memo has been recomputed.
    ///
    /// Unlike [`Signal::changed`], this triggers lazy recomputation if
    /// the memo is dirty, ensuring the returned value reflects the
    /// latest source signal state.
    pub async fn changed(&self) -> T {
        self.signal.changed().await;
        self.read()
    }

    /// Return `true` if any source signal has changed since the last
    /// recomputation.
    ///
    /// A dirty memo will recompute on the next [`read`](Memo::read) or
    /// [`with`](Memo::with) call.  This is a cheap flag check — it does
    /// not trigger computation.
    #[must_use]
    pub fn is_dirty(&self) -> bool {
        self.dirty.get()
    }

    /// Return the number of successful recomputations so far.
    ///
    /// Includes the initial compute performed during [`new`](Memo::new).
    /// Panicked recomputations are **not** counted.
    #[must_use]
    pub fn compute_count(&self) -> u64 {
        self.compute_count.get()
    }

    /// Return the opaque addresses of this memo's source signal
    /// dependencies, for use by diagnostic tools.
    ///
    /// Each address corresponds to the `state_addr` of a [`Signal`]
    /// that this memo reads during compute.  The returned set reflects
    /// the current dependency snapshot — it only updates after a
    /// successful recomputation.
    #[must_use]
    pub fn dependency_addrs(&self) -> Vec<usize> {
        self.subscriptions
            .borrow()
            .iter()
            .map(|(key, _)| key.addr())
            .collect()
    }

    /// Set a human-readable label for this memo.
    ///
    /// Labels appear in `dump_reactive_graph()` output and are useful
    /// for debugging.
    pub fn set_label(&self, label: impl Into<String>) {
        *self.label.borrow_mut() = Some(label.into());
    }

    /// Return the label set by [`set_label`](Self::set_label), if any.
    #[must_use]
    pub fn label(&self) -> Option<String> {
        self.label.borrow().clone()
    }

    // ------------------------------------------------------------------
    // internals
    // ------------------------------------------------------------------

    /// Re-run the compute function, incrementally updating subscriptions.
    ///
    /// Dependencies that appear in both the old and new subscription sets
    /// are kept — their unsubscribe closures are reused, avoiding churn
    /// on the signal's subscriber list.  Only removed dependencies are
    /// unsubscribed; only genuinely new dependencies trigger a fresh
    /// subscribe call.
    ///
    /// # Panic safety
    ///
    /// Old subscriptions are kept alive during compute.  New subscriptions
    /// are collected into a temporary list.  If `compute` panics, only
    /// the partial *new* subscriptions are cleaned up — the old set
    /// stays intact, keeping the memo connected to its sources.
    /// The `computing` flag suppresses `bump_version` during compute
    /// to prevent re-entrant reader wake-ups.
    fn recompute(&self) {
        // Prevent re-entrant recompute on the same memo.
        if self.computing.get() {
            return;
        }

        // Guard against circular Memo chains via recursion depth.
        let _depth = DepthGuard::enter();

        self.computing.set(true);

        // Collect old keys so the observer can skip already-subscribed
        // signals, avoiding duplicate subscribe/unsubscribe churn on
        // shared dependencies.
        let old_keys: HashSet<SignalKey> = self
            .subscriptions
            .borrow()
            .iter()
            .map(|(k, _)| *k)
            .collect();

        // Collect new subscriptions here; old ones stay live.
        let new_subs: SubscriptionList = Rc::new(RefCell::new(Vec::new()));
        let holder = Rc::new(RefCell::new(Some(self.signal.clone())));

        let t0 = crate::signal::now_us();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_compute(
                &self.compute,
                &self.dirty,
                &new_subs,
                &holder,
                &self.computing,
                &old_keys,
            )
        }));

        match result {
            Ok((new_value, _all_seen, re_read_keys)) => {
                let new_keys: HashSet<SignalKey> =
                    new_subs.borrow().iter().map(|(k, _)| *k).collect();
                let effective_read: HashSet<SignalKey> =
                    new_keys.union(&re_read_keys).copied().collect();

                let mut old = self.subscriptions.borrow_mut();
                let old_subs: Vec<(SignalKey, CleanupFn)> = std::mem::take(&mut *old);

                let mut keep = Vec::with_capacity(old_subs.len().max(effective_read.len()));
                for (key, cleanup) in old_subs {
                    if effective_read.contains(&key) {
                        keep.push((key, cleanup)); // still a dependency
                    } else {
                        cleanup(); // no longer read — unsubscribe
                    }
                }

                // Add genuinely new subscriptions; unsubscribe duplicates.
                for (key, cleanup) in new_subs.borrow_mut().drain(..) {
                    if old_keys.contains(&key) {
                        cleanup();
                    } else {
                        keep.push((key, cleanup));
                    }
                }

                *old = keep;
                drop(old);

                self.signal.set(new_value);
                self.dirty.set(false);
                self.compute_count
                    .set(self.compute_count.get().wrapping_add(1));
                let elapsed_us = crate::signal::now_us().saturating_sub(t0);
                self.last_compute_us.set(elapsed_us);
            }
            Err(payload) => {
                // Compute panicked — clean up partial new subscriptions.
                // Old subscriptions are untouched, so the memo stays
                // connected to its previous source set.
                for (_, cleanup) in new_subs.borrow_mut().drain(..) {
                    cleanup();
                }
                self.computing.set(false);
                std::panic::resume_unwind(payload);
            }
        }

        self.computing.set(false);
    }
}

// ---------------------------------------------------------------------------
// run_compute helper
// ---------------------------------------------------------------------------

/// Restore the previous OBSERVER on drop (panic-safe).
struct ObserverGuard {
    prev: Option<ObserverState>,
}

impl Drop for ObserverGuard {
    fn drop(&mut self) {
        OBSERVER.with(|cell| {
            *cell.borrow_mut() = self.prev.take();
        });
    }
}

/// Run `compute` with the observer installed so that every
/// [`Signal::read`] / [`Signal::with`] inside it auto-subscribes the
/// memo as a dependency.
///
/// The dirty callback both sets the dirty flag AND bumps the internal
/// signal's version so that nested memos (or other subscribers watching
/// this memo's output) are notified that the value may have changed.
///
/// `signal_holder` is an indirect reference to the memo's internal
/// [`Signal`].  During `Memo::new` the slot starts empty and is filled
/// after the signal is created; during `recompute` it already holds
/// the signal.  The indirection allows `new` to run `compute` exactly
/// once — before the signal exists — while still letting the dirty
/// callback bump the version afterward.
///
/// # Nested memo safety
///
/// The previous [`OBSERVER`] is saved before installing the new one and
/// restored afterward (even on panic, via [`ObserverGuard`]).  This
/// ensures that when a nested memo triggers a recomputation during the
/// outer memo's compute, the outer memo's observer is correctly
/// reinstalled afterward.
fn run_compute<T: Clone + 'static>(
    compute: &Rc<dyn Fn() -> T>,
    dirty: &Rc<Cell<bool>>,
    subscriptions: &SubscriptionList,
    signal_holder: &Rc<RefCell<Option<Signal<T>>>>,
    computing: &Rc<Cell<bool>>,
    pre_seen: &HashSet<SignalKey>,
) -> (T, HashSet<SignalKey>, HashSet<SignalKey>) {
    let dirty2 = Rc::clone(dirty);
    let subs = Rc::clone(subscriptions);
    let holder = Rc::clone(signal_holder);
    let computing2 = Rc::clone(computing);
    // Track which signals we've already subscribed to, so that
    // reading the same signal twice doesn't create duplicate subs.
    // Pre-populate with old dependencies so that shared signals are
    // not unsubscribed/resubscribed on every recomputation.
    let seen: Rc<RefCell<HashSet<SignalKey>>> = Rc::new(RefCell::new(pre_seen.clone()));
    let seen2 = Rc::clone(&seen);

    // Old keys that are actually re-read during this compute.
    let re_read: Rc<RefCell<HashSet<SignalKey>>> = Rc::new(RefCell::new(HashSet::new()));
    let re_read2 = Rc::clone(&re_read);

    let observer = ObserverState {
        dirty_callback: Rc::new(move || {
            // When computing is true, suppress both dirty and
            // bump_version: the recompute in progress will incorporate
            // any changes read before the source changed.  Changes
            // after reads (re-entrant external sets) are rare —
            // they require a synchronous callback in the no-hook
            // fallback path.  Suppressing dirty here prevents nested
            // memos from spuriously re-dirtying their parent after
            // the parent already read the new value.
            if !computing2.get() {
                dirty2.set(true);
                if let Some(ref sig) = *holder.borrow() {
                    sig.bump_version();
                }
            }
        }),
        on_subscribe: Rc::new(move |key: SignalKey, cleanup: Box<dyn FnOnce()>| {
            subs.borrow_mut().push((key, cleanup));
        }),
        seen: seen2,
        re_read: re_read2,
    };

    // Save previous observer, install ours, restore on scope exit.
    let prev = OBSERVER.with(|cell| cell.borrow_mut().take());
    let _guard = ObserverGuard { prev };

    OBSERVER.with(|cell| {
        *cell.borrow_mut() = Some(observer);
    });

    // _guard drops here, restoring the previous observer.
    let value = compute();
    let read_keys = seen.borrow().clone();
    let re_read_keys = re_read.borrow().clone();
    (value, read_keys, re_read_keys)
}

impl<T> Drop for Memo<T> {
    fn drop(&mut self) {
        // Only drain when this is the last clone — all clones share
        // the same `subscriptions` Rc.  Draining from any intermediate
        // clone would disconnect the remaining ones from their sources.
        if Rc::strong_count(&self.subscriptions) == 1 {
            for (_, cleanup) in self.subscriptions.borrow_mut().drain(..) {
                cleanup();
            }
        }
    }
}

impl<T> Clone for Memo<T> {
    fn clone(&self) -> Self {
        Self {
            signal: self.signal.clone(),
            dirty: Rc::clone(&self.dirty),
            compute: Rc::clone(&self.compute),
            subscriptions: Rc::clone(&self.subscriptions),
            computing: Rc::clone(&self.computing),
            compute_count: Rc::clone(&self.compute_count),
            label: Rc::clone(&self.label),
            last_compute_us: Rc::clone(&self.last_compute_us),
        }
    }
}

impl<T: fmt::Debug + 'static> fmt::Debug for Memo<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let subs = self.subscriptions.borrow().len();
        self.signal.with(|value| {
            let mut ds = f.debug_struct("Memo");
            if let Some(ref label) = self.label.borrow().as_ref() {
                ds.field("label", label);
            }
            ds.field("value", value)
                .field("dirty", &self.dirty.get())
                .field("subs", &subs)
                .finish_non_exhaustive()
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "memo_tests.rs"]
mod tests;
