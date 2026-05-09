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

type CleanupFn = Box<dyn FnOnce()>;
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

        let value = run_compute(&compute, &dirty, &subscriptions, &holder, &computing);

        let signal = Signal::new(value);
        *holder.borrow_mut() = Some(signal.clone());

        let memo = Self {
            signal,
            dirty,
            compute,
            subscriptions,
            computing,
            compute_count,
        };

        memo.dirty.set(false);
        memo.compute_count.set(1);
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
        // Prevent re-entrant recompute.
        if self.computing.get() {
            return;
        }
        self.computing.set(true);

        // Collect new subscriptions here; old ones stay live.
        let new_subs: SubscriptionList = Rc::new(RefCell::new(Vec::new()));
        let holder = Rc::new(RefCell::new(Some(self.signal.clone())));

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_compute(
                &self.compute,
                &self.dirty,
                &new_subs,
                &holder,
                &self.computing,
            )
        }));

        match result {
            Ok(new_value) => {
                // --- Incremental subscription diff ---
                //
                // Build a set of new SignalKeys for O(1) lookup.
                // Iterate old subscriptions in one pass, partitioning
                // into "keep" (shared) and "remove" (no longer a dep).
                // Then add truly new subscriptions (in new_subs but
                // not in old_keys).
                let new_keys: HashSet<SignalKey> =
                    new_subs.borrow().iter().map(|(k, _)| *k).collect();

                let mut old = self.subscriptions.borrow_mut();
                let old_subs: Vec<(SignalKey, CleanupFn)> = std::mem::take(&mut *old);

                let old_keys: HashSet<SignalKey> =
                    old_subs.iter().map(|(k, _)| *k).collect();

                let mut keep = Vec::with_capacity(old_subs.len().max(new_keys.len()));
                for (key, cleanup) in old_subs {
                    if new_keys.contains(&key) {
                        keep.push((key, cleanup)); // shared — reuse
                    } else {
                        cleanup(); // removed dependency — unsubscribe
                    }
                }

                // Add genuinely new subscriptions; unsubscribe duplicates.
                for (key, cleanup) in new_subs.borrow_mut().drain(..) {
                    if !old_keys.contains(&key) {
                        keep.push((key, cleanup));
                    } else {
                        // Same SignalKey → old subscription is already
                        // in `keep`.  Drop this duplicate to avoid
                        // accumulating subscribers on the source signal.
                        cleanup();
                    }
                }

                *old = keep;
                drop(old);

                self.signal.set(new_value);
                self.dirty.set(false);
                self.compute_count
                    .set(self.compute_count.get().wrapping_add(1));
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
) -> T {
    let dirty2 = Rc::clone(dirty);
    let subs = Rc::clone(subscriptions);
    let holder = Rc::clone(signal_holder);
    let computing2 = Rc::clone(computing);
    // Track which signals we've already subscribed to, so that
    // reading the same signal twice doesn't create duplicate subs.
    let seen: Rc<RefCell<HashSet<SignalKey>>> = Rc::new(RefCell::new(HashSet::new()));
    let seen2 = Rc::clone(&seen);

    let observer = ObserverState {
        dirty_callback: Rc::new(move || {
            dirty2.set(true);
            // Suppress version bumps while this memo is actively
            // recomputing — old subscriptions are still live and
            // could fire during compute, but we must not wake
            // readers until the new value is ready.
            if !computing2.get() {
                if let Some(ref sig) = *holder.borrow() {
                    sig.bump_version();
                }
            }
        }),
        on_subscribe: Rc::new(move |key: SignalKey, cleanup: Box<dyn FnOnce()>| {
            subs.borrow_mut().push((key, cleanup));
        }),
        seen: seen2,
    };

    // Save previous observer, install ours, restore on scope exit.
    let prev = OBSERVER.with(|cell| cell.borrow_mut().take());
    let _guard = ObserverGuard { prev };

    OBSERVER.with(|cell| {
        *cell.borrow_mut() = Some(observer);
    });

    // _guard drops here, restoring the previous observer.
    compute()
}

impl<T> Drop for Memo<T> {
    fn drop(&mut self) {
        for (_, cleanup) in self.subscriptions.borrow_mut().drain(..) {
            cleanup();
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
        }
    }
}

impl<T: fmt::Debug + 'static> fmt::Debug for Memo<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let subs = self.subscriptions.borrow().len();
        // Use with() to avoid the Clone bound on read().
        self.signal.with(|value| {
            f.debug_struct("Memo")
                .field("value", value)
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
mod tests {
    use super::*;
    use crate::Signal;

    #[test]
    fn memo_initial_value() {
        let a = Signal::new(2);
        let b = Signal::new(3);
        let sum = Memo::new(move || a.read() + b.read());
        assert_eq!(sum.read(), 5);
    }

    #[test]
    fn memo_lazy_recompute() {
        let a = Signal::new(1);
        let a2 = a.clone();
        let compute_count = Rc::new(Cell::new(0u32));
        let cc = Rc::clone(&compute_count);
        let doubled = Memo::new(move || {
            cc.set(cc.get() + 1);
            a2.read() * 2
        });
        // Memo::new calls compute once (observer installed before the call).
        assert_eq!(compute_count.get(), 1);
        assert_eq!(doubled.read(), 2);
        assert_eq!(compute_count.get(), 1); // no recompute (not dirty)

        a.set(10);
        assert_eq!(compute_count.get(), 1); // still lazy, not yet recomputed
        assert_eq!(doubled.read(), 20);
        assert_eq!(compute_count.get(), 2); // recomputed on read
    }

    #[test]
    fn memo_no_recompute_when_clean() {
        let a = Signal::new(5);
        let memo = Memo::new(move || a.read() * 2);
        assert_eq!(memo.read(), 10);
        assert_eq!(memo.read(), 10); // second read should not recompute
        assert_eq!(memo.read(), 10);
    }

    #[test]
    fn memo_multiple_sources() {
        let x = Signal::new(1);
        let y = Signal::new(2);
        let z = Signal::new(3);
        let x2 = x.clone();
        let y2 = y.clone();
        let z2 = z.clone();
        let sum = Memo::new(move || x2.read() + y2.read() + z2.read());
        assert_eq!(sum.read(), 6);

        x.set(10);
        assert_eq!(sum.read(), 15);

        y.set(20);
        assert_eq!(sum.read(), 33);

        z.set(30);
        assert_eq!(sum.read(), 60);
    }

    #[test]
    fn memo_changed_future() {
        let a = Signal::new(1);
        let a2 = a.clone();
        let doubled = Memo::new(move || a2.read() * 2);

        // Verify changed() compiles and returns correct value.
        // On the first call, the signal version hasn't changed yet,
        // so the future would be pending.  We just verify type-checking.
        let _fut = doubled.changed();

        // After source change and recompute, changed() should resolve.
        a.set(5);
        // After bump_version, the internal signal's version changed.
        // The changed future should now be ready (in a real async context).
        assert_eq!(doubled.read(), 10);
    }

    #[test]
    fn memo_drop_cleans_up() {
        let sig = Signal::new(0i32);
        {
            let s = sig.clone();
            let _memo = Memo::new(move || s.read() * 2);
            // After initial compute, the memo is subscribed to `sig`.
            assert!(sig.debug_count_waiters() > 0);
        }
        // After drop, the memo should have unsubscribed.
        assert_eq!(sig.debug_count_waiters(), 0);
    }

    #[test]
    fn memo_with_tracking() {
        let a = Signal::new(7);
        let memo = Memo::new(move || a.read() * 3);
        let result = memo.with(|v| *v);
        assert_eq!(result, 21);
    }

    #[test]
    fn memo_dependency_set_changes() {
        // The memo conditionally reads different signals.  When the
        // condition changes, old dependencies should be cleaned up and
        // new ones tracked.
        let use_b = Signal::new(false);
        let a = Signal::new(1);
        let b = Signal::new(100);

        let a2 = a.clone();
        let b2 = b.clone();
        let use_b2 = use_b.clone();
        let memo = Memo::new(move || if use_b2.read() { b2.read() } else { a2.read() });

        assert_eq!(memo.read(), 1);

        // While use_b is false, only `a` should be a dependency.
        // Changing `b` should not mark the memo dirty.
        b.set(200);
        assert_eq!(memo.read(), 1); // a hasn't changed

        // Now switch to b.
        use_b.set(true);
        assert_eq!(memo.read(), 200); // reads b's current value

        // Now `b` is a dependency; changing it should dirty the memo.
        b.set(300);
        assert_eq!(memo.read(), 300);

        // `a` is no longer a dependency; changing it should not affect
        // the memo.
        a.set(999);
        assert_eq!(memo.read(), 300);
    }

    #[test]
    fn memo_nested() {
        let base = Signal::new(2);
        let doubled = Memo::new({
            let b = base.clone();
            move || b.read() * 2
        });
        let quadrupled = Memo::new({
            let d = doubled.clone();
            move || d.read() * 2
        });

        assert_eq!(quadrupled.read(), 8);
        base.set(5);
        assert_eq!(quadrupled.read(), 20);
    }

    #[test]
    fn memo_stress_many_changes() {
        let sig = Signal::new(0i32);
        let memo = Memo::new({
            let s = sig.clone();
            move || s.read() * 2
        });

        for i in 1..=1000 {
            sig.set(i);
            assert_eq!(memo.read(), i * 2);
        }
    }

    #[test]
    fn memo_clone_shares_state() {
        let a = Signal::new(10);
        let a2 = a.clone();
        let m1 = Memo::new(move || a2.read() * 2);
        let m2 = m1.clone();

        assert_eq!(m2.read(), 20);
        a.set(15);
        // Both clones see the same dirty state.
        assert_eq!(m1.read(), 30);
        assert_eq!(m2.read(), 30);
    }

    #[test]
    fn memo_is_dirty_flag() {
        let a = Signal::new(1);
        let a2 = a.clone();
        let memo = Memo::new(move || a2.read() * 2);

        // After construction, memo is clean.
        assert!(!memo.is_dirty());

        // Source change marks memo dirty.
        a.set(5);
        assert!(memo.is_dirty());

        // read() clears the dirty flag.
        assert_eq!(memo.read(), 10);
        assert!(!memo.is_dirty());
    }

    #[test]
    fn memo_compute_count_increments() {
        let a = Signal::new(1);
        let a2 = a.clone();
        let memo = Memo::new(move || a2.read() * 2);

        // Initial compute counts as 1.
        assert_eq!(memo.compute_count(), 1);

        // read() without source change does NOT recompute.
        let _ = memo.read();
        assert_eq!(memo.compute_count(), 1);

        // Source change + read triggers recompute.
        a.set(10);
        let _ = memo.read();
        assert_eq!(memo.compute_count(), 2);

        // Clones share the same counter.
        let clone = memo.clone();
        assert_eq!(clone.compute_count(), 2);
        a.set(20);
        let _ = clone.read();
        assert_eq!(memo.compute_count(), 3);
    }
}
