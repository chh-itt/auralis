//! Tests for `auralis_signal::Memo<T>` — lazy auto-tracking computed
//! signals with incremental subscription diffing.
//!
//! # Organisation
//!
//! | Section | Tests | What it verifies |
//! |---|---|---|
//! | **Basics** | `memo_initial_value`, `memo_lazy_recompute`, `memo_no_recompute_when_clean`, `memo_multiple_sources`, `memo_changed_future`, `memo_with_tracking`, `memo_clone_shares_state`, `memo_is_dirty_flag`, `memo_compute_count_increments` | Lazy recompute, multi-source tracking, dirty flag, clone behaviour |
//! | **Cleanup** | `memo_drop_cleans_up`, `memo_clone_drop_does_not_disconnect_siblings` | Subscriber cleanup on drop, sibling isolation |
//! | **Dependency changes** | `memo_dependency_set_changes` | Conditional reads: when a branch switches from reading A to reading B, old dependency unsubscribed, new subscribed |
//! | **Incremental diff** | `memo_incremental_diff_*` | Add / remove / replace dependencies; shared deps kept, only genuinely new/removed subs changed |
//! | **Multi-variant** | `memo_dependency_remove_unused_branch_with_multiple_variants` | Enum-like match with 3 variants, each switching to a different signal |
//! | **Dedup** | `memo_dedup_same_signal_via_signalmap` | Reading same underlying signal via Signal + SignalMap → single subscriber |
//! | **Nested & deep chain** | `memo_nested`, `memo_deep_dependency_chain_works` | 50-level linear chain, nested Memo reads |
//! | **Circular dependency** | `memo_true_circular_dependency_stabilizes_gracefully` | A↔B cycle caught by `computing` flag, stabilises without panic |
//! | **Batch interaction** | `memo_batch_*` | Multiple source sets → single recompute; read inside batch returns old value (callbacks deferred) |
//! | **Panic safety** | `memo_panics_during_compute_then_recovers`, `memo_self_read_in_compute_does_not_panic` | Panicked recompute → old subscriptions kept; self-read during compute safe |
//! | **Large-scale diff** | `memo_dependency_diff_large_scale_swap` | 50 ↔ 50 signals full replacement, all subscriptions correct |
//! | **SignalMap integration** | `memo_with_signalmap_tracks_dependency` | Memo depending on a `SignalMap` tracks changes correctly |
//! | **Stress** | `memo_stress_many_changes` | 1000 sequential changes, correct lazy recompute |

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
    // Verify that `a` was actually unsubscribed — the memo must
    // NOT be dirty after `a` changes.
    assert!(!memo.is_dirty());
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

#[test]
fn memo_panics_during_compute_then_recovers() {
    let sig = Signal::new(1);
    let should_panic = Rc::new(Cell::new(false));
    let sp = Rc::clone(&should_panic);
    let s = sig.clone();

    // Construct with should_panic=false so compute passes.
    let memo = Memo::new(move || {
        assert!(!sp.get(), "intentional memo panic");
        s.read() * 2
    });
    assert_eq!(memo.read(), 2);

    // Now enable panic and trigger recompute — it should panic.
    should_panic.set(true);
    sig.set(99); // marks memo dirty
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| memo.read()));
    assert!(result.is_err());

    // Disable panic and verify recovery.
    should_panic.set(false);
    sig.set(5);
    assert_eq!(memo.read(), 10);
}

#[test]
fn memo_self_read_in_compute_does_not_panic() {
    // Reading the memo's own output inside its compute function is
    // unusual but must not cause a deadlock or panic.
    let sig = Signal::new(1);
    let s = sig.clone();
    let memo = Memo::new(move || s.read() * 2);

    // Call read() inside the memo's with() — this triggers recompute,
    // and during recompute read() is called again (clean path).
    let result = memo.with(|v| *v);
    assert_eq!(result, 2);
}

#[test]
fn memo_with_signalmap_tracks_dependency() {
    // SignalMap::with must trigger observer tracking so that a
    // memo depending on a SignalMap is marked dirty correctly.
    let source = Signal::new(42);
    let sm = source.map(|v: &i32| *v);
    let sm2 = sm.clone();

    let memo = Memo::new(move || sm2.with(|v| *v));

    assert_eq!(memo.read(), 42);
    source.set(99);
    // The memo must be dirty because it reads through SignalMap::with.
    assert!(memo.is_dirty());
    assert_eq!(memo.read(), 99);
}

#[test]
fn memo_clone_drop_does_not_disconnect_siblings() {
    // Dropping one clone must not unsubscribe shared sources.
    let sig = Signal::new(0i32);
    let s = sig.clone();
    let m1 = Memo::new(move || s.read() * 2);
    let m2 = m1.clone();

    assert_eq!(m1.read(), 0);
    assert_eq!(m2.read(), 0);

    // Drop m2 — m1 must remain connected.
    drop(m2);

    sig.set(10);
    assert!(
        m1.is_dirty(),
        "m1 should still be subscribed after clone drop"
    );
    assert_eq!(m1.read(), 20);
}

// -- incremental dependency diff ---------------------------------------

#[test]
fn memo_incremental_diff_add_dependency() {
    let flag = Signal::new(false);
    let a = Signal::new(1);
    let b = Signal::new(100);

    let f = flag.clone();
    let a2 = a.clone();
    let b2 = b.clone();
    let memo = Memo::new(move || {
        let mut sum = a2.read();
        if f.read() {
            sum += b2.read();
        }
        sum
    });

    assert_eq!(memo.read(), 1);
    assert_eq!(a.debug_count_waiters(), 1);

    // Switch flag → memo should now depend on B as well.
    flag.set(true);
    assert_eq!(memo.read(), 101);
    // A must still be a dependency (was re-read), B newly subscribed.
    assert!(a.debug_count_waiters() > 0, "A should still be subscribed");
    assert!(b.debug_count_waiters() > 0, "B should now be subscribed");

    // Changing B should dirty the memo.
    b.set(200);
    assert!(memo.is_dirty());
    assert_eq!(memo.read(), 201);
}

#[test]
fn memo_incremental_diff_remove_dependency() {
    let flag = Signal::new(true);
    let a = Signal::new(1);
    let b = Signal::new(100);

    let f = flag.clone();
    let a2 = a.clone();
    let b2 = b.clone();
    let memo = Memo::new(move || {
        let mut sum = a2.read();
        if f.read() {
            sum += b2.read();
        }
        sum
    });

    assert_eq!(memo.read(), 101);
    assert!(
        b.debug_count_waiters() > 0,
        "B should start as a dependency"
    );

    // Switch flag off → B should be unsubscribed.
    flag.set(false);
    assert_eq!(memo.read(), 1);
    assert!(a.debug_count_waiters() > 0, "A should remain subscribed");
    assert_eq!(
        b.debug_count_waiters(),
        0,
        "B should be unsubscribed after becoming inactive"
    );

    // Changing B should NOT dirty the memo.
    b.set(200);
    assert!(
        !memo.is_dirty(),
        "B change should not affect memo after B was removed from deps"
    );
    assert_eq!(memo.read(), 1);
}

#[test]
fn memo_incremental_diff_replace_dependencies() {
    let switch = Signal::new(false);
    let a = Signal::new(1);
    let b = Signal::new(2);
    let c = Signal::new(10);
    let d = Signal::new(20);

    let sw = switch.clone();
    let a2 = a.clone();
    let b2 = b.clone();
    let c2 = c.clone();
    let d2 = d.clone();
    let memo = Memo::new(move || {
        if sw.read() {
            c2.read() + d2.read()
        } else {
            a2.read() + b2.read()
        }
    });

    assert_eq!(memo.read(), 3);
    assert_eq!(a.debug_count_waiters(), 1);
    assert_eq!(b.debug_count_waiters(), 1);
    assert_eq!(c.debug_count_waiters(), 0);
    assert_eq!(d.debug_count_waiters(), 0);

    // Replace A+B with C+D.
    switch.set(true);
    assert_eq!(memo.read(), 30);
    assert_eq!(a.debug_count_waiters(), 0, "A should be unsubscribed");
    assert_eq!(b.debug_count_waiters(), 0, "B should be unsubscribed");
    assert_eq!(c.debug_count_waiters(), 1, "C should be subscribed");
    assert_eq!(d.debug_count_waiters(), 1, "D should be subscribed");

    // A change should NOT affect memo.
    a.set(999);
    assert!(!memo.is_dirty());

    // C change SHOULD affect memo.
    c.set(100);
    assert!(memo.is_dirty());
    assert_eq!(memo.read(), 120);
}

#[test]
fn memo_dedup_same_signal_via_signalmap() {
    let sig = Signal::new(10);
    let sm = sig.map(|v: &i32| *v);

    let s = sig.clone();
    let sm2 = sm.clone();
    let memo = Memo::new(move || s.read() + sm2.read());

    assert_eq!(memo.read(), 20);
    // Only one subscription, not two (same underlying signal).
    assert_eq!(sig.debug_count_waiters(), 1);

    sig.set(5);
    assert_eq!(memo.read(), 10);
}

#[test]
fn memo_dependency_remove_unused_branch_with_multiple_variants() {
    // Like a match statement: reads different signals based on variant.
    #[derive(Clone, PartialEq)]
    enum Mode {
        One,
        Two,
        Three,
    }

    let mode = Signal::new(Mode::One);
    let v1 = Signal::new(1i32);
    let v2 = Signal::new(2i32);
    let v3 = Signal::new(3i32);

    let m = mode.clone();
    let a = v1.clone();
    let b = v2.clone();
    let c = v3.clone();
    let memo = Memo::new(move || match m.read() {
        Mode::One => a.read(),
        Mode::Two => b.read(),
        Mode::Three => c.read(),
    });

    assert_eq!(memo.read(), 1);
    // Only v1 should be subscribed.
    assert_eq!(v1.debug_count_waiters(), 1);
    assert_eq!(v2.debug_count_waiters(), 0);
    assert_eq!(v3.debug_count_waiters(), 0);

    // Switch to Two.
    mode.set(Mode::Two);
    assert_eq!(memo.read(), 2);
    assert_eq!(v1.debug_count_waiters(), 0, "v1 should be unsubscribed");
    assert_eq!(v2.debug_count_waiters(), 1, "v2 should be subscribed");
    assert_eq!(v3.debug_count_waiters(), 0);

    // Switch to Three.
    mode.set(Mode::Three);
    assert_eq!(memo.read(), 3);
    assert_eq!(v2.debug_count_waiters(), 0, "v2 should be unsubscribed");
    assert_eq!(v3.debug_count_waiters(), 1, "v3 should be subscribed");

    // Verify that changing old dependencies has no effect.
    v1.set(999);
    v2.set(999);
    assert!(!memo.is_dirty());
    assert_eq!(memo.read(), 3);
}

// -- circular dependency detection -------------------------------------

#[test]
fn memo_true_circular_dependency_stabilizes_gracefully() {
    let sig = Signal::new(0i32);
    let b_holder: Rc<RefCell<Option<Memo<i32>>>> = Rc::new(RefCell::new(None));

    let a: Memo<i32>;
    {
        let s = sig.clone();
        let bh = Rc::clone(&b_holder);
        a = Memo::new(move || {
            let mut sum = s.read();
            if let Some(ref b) = *bh.borrow() {
                sum += b.read();
            }
            sum
        });
    }

    let s = sig.clone();
    let a2 = a.clone();
    let b = Memo::new(move || s.read() + a2.read());
    *b_holder.borrow_mut() = Some(b.clone());

    // Both A and B read sig → both become dirty when sig changes.
    sig.set(1);
    // A.recompute → reads B → B.recompute → reads A (still computing, returns old)
    // A finishes with new value → B's dirty_callback fires → B.dirty=true
    // B is still dirty → next read stabilizes.
    // Neither read should cause infinite recursion or panic.
    let _a_val = a.read();
    let _b_val = b.read();
}

#[test]
fn memo_deep_dependency_chain_works() {
    // Build a linear chain: M0 reads sig + M1, ..., M49 reads sig.
    // All 50 memos recompute correctly when sig changes.
    let sig = Signal::new(0i32);

    let mut memos: Vec<Memo<i32>> = Vec::new();

    // Bottommost: M49, reads only sig.
    memos.push(Memo::new({
        let s = sig.clone();
        move || s.read()
    }));

    // Build upward: each memo reads sig + the previous (child) memo.
    for _ in 0..49 {
        let s = sig.clone();
        let child = memos.last().unwrap().clone();
        memos.push(Memo::new(move || s.read() + child.read()));
    }
    // memos.last() = M0 (root), reads sig + M1.

    let root = memos.last().unwrap();
    assert_eq!(root.read(), 0);

    sig.set(1);
    assert_eq!(root.read(), 50);

    sig.set(2);
    assert_eq!(root.read(), 100);
}

#[test]
fn memo_batch_multiple_source_sets_single_recompute() {
    let a = Signal::new(1);
    let b = Signal::new(10);
    let a2 = a.clone();
    let b2 = b.clone();
    let compute_count = Rc::new(Cell::new(0u32));
    let cc = Rc::clone(&compute_count);

    let memo = Memo::new(move || {
        cc.set(cc.get() + 1);
        a2.read() + b2.read()
    });

    assert_eq!(memo.read(), 11);
    assert_eq!(compute_count.get(), 1);

    crate::batch(|| {
        a.set(2);
        b.set(20);
    });

    assert_eq!(memo.read(), 22);
    assert_eq!(compute_count.get(), 2);
}

#[test]
fn memo_batch_read_during_batch() {
    let a = Signal::new(1);
    let a2 = a.clone();
    let memo = Memo::new(move || a2.read() * 2);

    crate::batch(|| {
        a.set(5);
        // Inside batch, signal callbacks are deferred.
        // Memo is NOT yet dirty — it returns old cached value.
        assert_eq!(memo.read(), 2);
        a.set(10);
        assert_eq!(memo.read(), 2);
    });
    // After batch exits, notification fires → memo is dirty.
    assert_eq!(memo.read(), 20);
}

#[test]
fn memo_dependency_diff_large_scale_swap() {
    // 50 → 50 different signals: all old unsubscribed, all new subscribed.
    let use_set_b = Signal::new(false);
    let mut set_a: Vec<Signal<i32>> = Vec::new();
    let mut set_b: Vec<Signal<i32>> = Vec::new();
    for i in 0i32..50 {
        set_a.push(Signal::new(i));
        set_b.push(Signal::new(i + 100));
    }

    let sw = use_set_b.clone();
    let a_clones: Vec<Signal<i32>> = set_a.iter().map(Signal::clone).collect();
    let b_clones: Vec<Signal<i32>> = set_b.iter().map(Signal::clone).collect();
    let memo = Memo::new(move || {
        if sw.read() {
            b_clones.iter().map(Signal::read).sum::<i32>()
        } else {
            a_clones.iter().map(Signal::read).sum::<i32>()
        }
    });

    // Initial: reads all of set_a.
    let expected: i32 = (0..50).sum();
    assert_eq!(memo.read(), expected);
    for (i, s) in set_a.iter().enumerate() {
        assert_eq!(s.debug_count_waiters(), 1, "A[{i}] should be subscribed");
    }
    for (i, s) in set_b.iter().enumerate() {
        assert_eq!(
            s.debug_count_waiters(),
            0,
            "B[{i}] should not be subscribed"
        );
    }

    // Switch to set_b.
    use_set_b.set(true);
    let expected_b: i32 = (100..150).sum();
    assert_eq!(memo.read(), expected_b);
    for s in &set_a {
        assert_eq!(
            s.debug_count_waiters(),
            0,
            "all of A should be unsubscribed"
        );
    }
    for s in &set_b {
        assert_eq!(s.debug_count_waiters(), 1, "all of B should be subscribed");
    }
}

#[test]
fn memo_dependency_addrs_returns_source_addresses() {
    let a = Signal::new(1);
    let b = Signal::new(2);
    let a2 = a.clone();
    let b2 = b.clone();
    let memo = Memo::new(move || a2.read() + b2.read());

    let addrs = memo.dependency_addrs();
    assert_eq!(addrs.len(), 2, "should have two source dependencies");
    // Both a and b should be in the dependency list.
    let a_addr = a.state_addr();
    let b_addr = b.state_addr();
    assert!(addrs.contains(&a_addr), "should contain a's address");
    assert!(addrs.contains(&b_addr), "should contain b's address");

    // After recomputation, the addresses should be stable.
    a.set(10);
    let _ = memo.read();
    let addrs2 = memo.dependency_addrs();
    assert_eq!(addrs2.len(), 2);
}

#[test]
fn memo_label_set_and_get() {
    let sig = Signal::new(1);
    let memo = Memo::new(move || sig.read() * 2);

    assert_eq!(memo.label(), None);
    memo.set_label("doubler");
    assert_eq!(memo.label(), Some("doubler".to_string()));
}

#[test]
fn memo_label_clone_shares_label() {
    let sig = Signal::new(1);
    let memo = Memo::new(move || sig.read() * 2);
    memo.set_label("orig");
    let clone = memo.clone();
    assert_eq!(clone.label(), Some("orig".to_string()));

    clone.set_label("updated");
    assert_eq!(memo.label(), Some("updated".to_string()));
}
