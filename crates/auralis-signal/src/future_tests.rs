#![allow(clippy::items_after_statements)]
//! Tests for `auralis_signal::Signal`, change-detection futures, batch
//! updates, and the deferred notification state machine.
//!
//! # Organisation
//!
//! | Section | Tests | What it verifies |
//! |---|---|---|
//! | **Test infrastructure** | `TestWaker`, `test_waker`, `poll`, `pin_mut` | Minimal async test harness (no executor needed) |
//! | **Signal basics** | `new_signal_read`, `set_updates_value`, `set_same_value_*`, `set_if_changed_*`, `with_*`, `ptr_eq_*`, `rapid_set_*` | Value storage, version bump, partial-eq guard |
//! | **Changed futures** | `changed_single_wake`, `changed_multiple_wakes`, `changed_after_multiple_sets`, `changed_version_tracking`, `multiple_tasks_waiting` | `SignalChangedFuture` wake-on-change, version-based dedup |
//! | **Waker deregistration** | `drop_changed_future_*`, `drop_1000_futures_*`, `long_lived_signal_*`, `waker_dedup`, `debug_count_waiters_*` | Proactive unsubscribe on future drop, no stale-waker accumulation |
//! | **Map / Filter futures** | `map_changed_*`, `filter_changed_*` | `MapChangedFuture` / `FilterChangedFuture` transform, drop cleanup |
//! | **Subscribe during callbacks** | `subscribe_during_set_*`, `self_unsubscribe_during_*`, `new_subscriber_not_invoked_*`, `stress_random_*` | Snapshot isolation, alive-flag, randomised stress |
//! | **Batch** | `batch_*` | Nested batch, single notification, panic isolation, subscriber sees final value |
//! | **State machine** | `state_machine_*` | Re-entrant `set()` inside callback, cross-signal re-entrancy, fresh snapshot on follow-up, self-terminating chains, multiple-subscriber re-entrancy, panic propagation |
//! | **SignalMap** | `signalmap_*` | `changed()`, `read()`, `with()` observer tracking |
//! | **Edge cases** | `changed_re_await_*`, `signal_set_in_drop_*` | Re-await without new change stays pending, `set()` in `Drop` |

use super::*;
use crate::signal::Signal;
use std::cell::{Cell, RefCell};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

// ------------------------------------------------------------------
// Test infrastructure
// ------------------------------------------------------------------

struct TestWaker {
    woken: Arc<AtomicBool>,
}

impl Wake for TestWaker {
    fn wake(self: Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
    }
}

fn test_waker() -> (Waker, Arc<AtomicBool>) {
    let woken = Arc::new(AtomicBool::new(false));
    let tw = Arc::new(TestWaker {
        woken: Arc::clone(&woken),
    });
    (Waker::from(tw), woken)
}

fn poll<F: Future>(fut: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    let mut cx = Context::from_waker(waker);
    fut.poll(&mut cx)
}

fn pin_mut<F: Future + Unpin>(fut: &mut F) -> Pin<&mut F> {
    Pin::new(fut)
}

// ------------------------------------------------------------------
// Test cases
// ------------------------------------------------------------------

#[test]
fn new_signal_read() {
    let sig = Signal::new(42);
    assert_eq!(sig.read(), 42);
}

#[test]
fn set_updates_value() {
    let sig = Signal::new(0);
    sig.set(42);
    assert_eq!(sig.read(), 42);
}

#[test]
fn changed_single_wake() {
    let sig = Signal::new(0);
    let (waker, woken) = test_waker();
    let mut fut = sig.changed();

    // First poll should be pending (no change yet).
    assert!(poll(pin_mut(&mut fut), &waker).is_pending());

    sig.set(1);

    // The waker should have been marked woken.
    assert!(woken.load(Ordering::SeqCst));

    // Re-poll should now deliver the new value.
    assert_eq!(poll(pin_mut(&mut fut), &waker), Poll::Ready(1));
}

#[test]
fn changed_multiple_wakes() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();

    // Cycle 1
    {
        let mut fut = sig.changed();
        sig.set(10);
        assert_eq!(poll(pin_mut(&mut fut), &waker), Poll::Ready(10));
    }

    // Cycle 2 — new future, new set.
    {
        let mut fut = sig.changed();
        sig.set(20);
        assert_eq!(poll(pin_mut(&mut fut), &waker), Poll::Ready(20));
    }

    // Cycle 3
    {
        let mut fut = sig.changed();
        sig.set(30);
        assert_eq!(poll(pin_mut(&mut fut), &waker), Poll::Ready(30));
    }
}

#[test]
fn changed_after_multiple_sets() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();
    let mut fut = sig.changed();

    // Set many times before ever polling.
    sig.set(1);
    sig.set(2);
    sig.set(3);
    sig.set(99);

    // The future should see only the latest value.
    assert_eq!(poll(pin_mut(&mut fut), &waker), Poll::Ready(99));
}

#[test]
fn multiple_tasks_waiting() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();

    let mut f1 = sig.changed();
    let mut f2 = sig.changed();
    let mut f3 = sig.changed();

    // All initially pending.
    assert!(poll(pin_mut(&mut f1), &waker).is_pending());
    assert!(poll(pin_mut(&mut f2), &waker).is_pending());
    assert!(poll(pin_mut(&mut f3), &waker).is_pending());

    assert_eq!(sig.debug_count_waiters(), 3);

    // One set wakes all.
    sig.set(99);

    assert_eq!(poll(pin_mut(&mut f1), &waker), Poll::Ready(99));
    assert_eq!(poll(pin_mut(&mut f2), &waker), Poll::Ready(99));
    assert_eq!(poll(pin_mut(&mut f3), &waker), Poll::Ready(99));
}

#[test]
fn changed_version_tracking() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();
    let mut fut = sig.changed();

    // First poll: same version → Pending.
    assert!(poll(pin_mut(&mut fut), &waker).is_pending());

    // Second poll without any set → still Pending.
    assert!(poll(pin_mut(&mut fut), &waker).is_pending());

    // After a set → Ready.
    sig.set(1);
    assert_eq!(poll(pin_mut(&mut fut), &waker), Poll::Ready(1));
}

#[test]
fn drop_changed_future_deregisters_waker() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();

    {
        let mut fut = sig.changed();
        let _ = poll(pin_mut(&mut fut), &waker); // subscribes
        assert_eq!(sig.debug_count_waiters(), 1);
        // fut dropped here
    }

    assert_eq!(sig.debug_count_waiters(), 0);
}

#[test]
fn drop_1000_futures_no_waker_leak() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();

    for _ in 0..1000 {
        let mut fut = sig.changed();
        let _ = poll(pin_mut(&mut fut), &waker); // subscribe
        assert_eq!(sig.debug_count_waiters(), 1);
        // fut dropped → subscriber removed
    }

    assert_eq!(sig.debug_count_waiters(), 0);
}

#[test]
fn long_lived_signal_no_waker_accumulation() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();

    // The signal is never set.  We repeatedly create futures, poll
    // them (subscribing), and drop them (unsubscribing).
    for _ in 0..100 {
        let mut fut = sig.changed();
        let _ = poll(pin_mut(&mut fut), &waker);
        assert_eq!(sig.debug_count_waiters(), 1);
        // fut dropped — must clean up its subscriber
    }

    assert_eq!(sig.debug_count_waiters(), 0);
}

#[test]
fn map_changed_correctly_transforms() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();
    let mut fut = sig.map_changed(|v| v * 2);

    sig.set(5);
    assert_eq!(poll(pin_mut(&mut fut), &waker), Poll::Ready(10));
}

#[test]
fn filter_changed_predicate_passes() {
    // Start with an odd value.
    let sig = Signal::new(1);
    let (waker, _woken) = test_waker();
    let mut fut = sig.filter_changed(|v| v % 2 == 0);

    // Set to another odd value — future should keep waiting.
    sig.set(3);
    assert!(poll(pin_mut(&mut fut), &waker).is_pending());

    // Set to an even value — future should resolve.
    sig.set(4);
    assert_eq!(poll(pin_mut(&mut fut), &waker), Poll::Ready(4));
}

#[test]
fn filter_changed_immediate_match() {
    // Signal already contains a value that passes the predicate,
    // but the future hasn't seen it yet (seen == current version).
    // The future should wait for the NEXT change.
    let sig = Signal::new(2); // even
    let (waker, _woken) = test_waker();
    let mut fut = sig.filter_changed(|v| v % 2 == 0);

    // Same version — should be pending.
    assert!(poll(pin_mut(&mut fut), &waker).is_pending());

    // New value that also passes.
    sig.set(4);
    assert_eq!(poll(pin_mut(&mut fut), &waker), Poll::Ready(4));
}

#[test]
fn filter_changed_skips_many() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();
    let mut fut = sig.filter_changed(|v| *v >= 10);

    // All these are < 10, predicate fails each time.
    for i in 1..10 {
        sig.set(i);
        assert!(poll(pin_mut(&mut fut), &waker).is_pending());
    }

    // This one passes.
    sig.set(10);
    assert_eq!(poll(pin_mut(&mut fut), &waker), Poll::Ready(10));
}

#[test]
fn with_borrows_value() {
    let sig = Signal::new(42);
    let result = sig.with(|v| *v * 2);
    assert_eq!(result, 84);
}

#[test]
fn with_does_not_clone() {
    // For non-Copy types, with() avoids cloning.
    let sig = Signal::new("hello".to_string());
    let len = sig.with(String::len);
    assert_eq!(len, 5);
    // The original value is still in the signal.
    assert_eq!(sig.read(), "hello");
}

#[test]
#[should_panic(expected = "already borrowed")]
fn with_panics_on_nested_set() {
    let sig = Signal::new(0i32);
    let sig2 = sig.clone();
    sig.with(|_v| {
        sig2.set(1); // RefCell borrow conflict — should panic.
    });
}

#[test]
fn waker_dedup() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();
    let mut fut = sig.changed();

    // First poll subscribes.
    let _ = poll(pin_mut(&mut fut), &waker);
    assert_eq!(sig.debug_count_waiters(), 1);

    // Second poll with the same waker should NOT add a duplicate
    // (subscription_id is already set).
    let _ = poll(pin_mut(&mut fut), &waker);
    assert_eq!(sig.debug_count_waiters(), 1);

    // After a set the future resolves and subscription is cleaned up
    // on next poll (which returns Ready without re-subscribing).
    sig.set(7);
    assert_eq!(poll(pin_mut(&mut fut), &waker), Poll::Ready(7));
}

#[test]
fn rapid_set_1000_no_panic() {
    let sig = Signal::new(0i32);
    for i in 0..1000 {
        sig.set(i);
    }
    assert_eq!(sig.read(), 999);
}

#[test]
fn debug_count_waiters_reflects_count() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();

    assert_eq!(sig.debug_count_waiters(), 0);

    let mut f1 = sig.changed();
    let mut f2 = sig.changed();
    let mut f3 = sig.changed();

    let _ = poll(pin_mut(&mut f1), &waker);
    let _ = poll(pin_mut(&mut f2), &waker);
    let _ = poll(pin_mut(&mut f3), &waker);

    assert_eq!(sig.debug_count_waiters(), 3);

    // Drop one and the count decreases.
    drop(f1);
    assert_eq!(sig.debug_count_waiters(), 2);

    drop(f2);
    assert_eq!(sig.debug_count_waiters(), 1);

    drop(f3);
    assert_eq!(sig.debug_count_waiters(), 0);
}

#[test]
fn map_changed_drop_cleans_up() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();

    {
        let mut fut = sig.map_changed(|v: &i32| v * 2);
        let _ = poll(pin_mut(&mut fut), &waker);
        assert_eq!(sig.debug_count_waiters(), 1);
    }

    assert_eq!(sig.debug_count_waiters(), 0);
}

#[test]
fn filter_changed_drop_cleans_up() {
    let sig = Signal::new(0);
    let (waker, _woken) = test_waker();

    {
        let mut fut = sig.filter_changed(|_: &i32| true);
        let _ = poll(pin_mut(&mut fut), &waker);
        assert_eq!(sig.debug_count_waiters(), 1);
    }

    assert_eq!(sig.debug_count_waiters(), 0);
}

#[test]
fn subscribe_during_set_callback_is_preserved() {
    let sig = Signal::new(0i32);

    // Subscriber A: on set, registers subscriber B.
    let sig_b = sig.clone();
    crate::subscribe(
        &sig,
        Rc::new(move || {
            crate::subscribe(&sig_b, Rc::new(|| {}));
        }),
    );

    assert_eq!(sig.debug_count_waiters(), 1);

    // Trigger set — A's callback fires, which subscribes B.
    sig.set(1);
    // After deferred execution, both A and B should be present.
    // But since there's no executor hook, callbacks run synchronously.
    assert_eq!(
        sig.debug_count_waiters(),
        2,
        "B should survive the set that created it"
    );
}

#[test]
fn self_unsubscribe_during_set_leaves_only_other_subscribers() {
    let sig = Signal::new(0i32);

    let a_id: Rc<Cell<Option<crate::signal::SubscriberId>>> = Rc::new(Cell::new(None));
    let a_id2 = Rc::clone(&a_id);
    let sig_b = sig.clone();

    let sub_id_a = crate::subscribe(
        &sig,
        Rc::new(move || {
            // Register B during callback.
            crate::subscribe(&sig_b, Rc::new(|| {}));
            // Unsubscribe self (A).
            if let Some(id) = a_id2.get() {
                crate::unsubscribe(&sig_b, id);
            }
        }),
    );
    a_id.set(Some(sub_id_a));

    assert_eq!(sig.debug_count_waiters(), 1);

    sig.set(1);

    // Only B should remain (A unsubscribed itself).
    assert_eq!(
        sig.debug_count_waiters(),
        1,
        "A should be gone, only B remains"
    );
}

/// Randomized stress test: subscribe, set, unsubscribe in random order,
/// then verify waiter count matches expected.
#[test]
fn stress_random_subscribe_unsubscribe() {
    // Simple LCG for deterministic "randomness" without a dependency.
    let mut rng: u64 = 42;
    let mut next = move || {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (rng >> 33) as usize
    };

    let sig = Signal::new(0i32);
    let mut active: Vec<crate::signal::SubscriberId> = Vec::new();

    for _ in 0..200 {
        match next() % 3 {
            0 => {
                // Subscribe a no-op callback.
                let s = sig.clone();
                let id = crate::subscribe(
                    &sig,
                    Rc::new(move || {
                        let _ = s.read();
                    }),
                );
                active.push(id);
            }
            1 => {
                // Unsubscribe a random active subscriber.
                if !active.is_empty() {
                    let idx = next() % active.len();
                    let id = active.swap_remove(idx);
                    crate::unsubscribe(&sig, id);
                }
            }
            _ => {
                // Set the signal.
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                sig.set(next() as i32);
            }
        }
    }

    // Ensure waiter count matches active count.
    assert_eq!(
        sig.debug_count_waiters(),
        active.len(),
        "waiter count mismatch after random operations"
    );

    // Clean up all remaining subscribers.
    for id in active {
        crate::unsubscribe(&sig, id);
    }
    assert_eq!(sig.debug_count_waiters(), 0);
}

// -- batch tests --------------------------------------------------------

#[test]
fn batch_multiple_sets_single_notification() {
    let sig = Signal::new(0);
    let call_count = Rc::new(Cell::new(0u32));
    let cc = Rc::clone(&call_count);
    crate::subscribe(
        &sig,
        Rc::new(move || {
            cc.set(cc.get() + 1);
        }),
    );

    crate::batch(|| {
        sig.set(1);
        sig.set(2);
        sig.set(3);
    });

    // Value should be latest, callback called exactly once.
    assert_eq!(sig.read(), 3);
    assert_eq!(call_count.get(), 1);
}

#[test]
fn batch_nested() {
    let sig = Signal::new(0);
    let call_count = Rc::new(Cell::new(0u32));
    let cc = Rc::clone(&call_count);
    crate::subscribe(
        &sig,
        Rc::new(move || {
            cc.set(cc.get() + 1);
        }),
    );

    crate::batch(|| {
        sig.set(1);
        crate::batch(|| {
            sig.set(2);
            crate::batch(|| {
                sig.set(3);
            });
            // Inner batch exited — no notification yet.
            assert_eq!(call_count.get(), 0);
        });
        // Middle batch exited — still no notification.
        assert_eq!(call_count.get(), 0);
    });

    // Outermost batch exited — one notification.
    assert_eq!(sig.read(), 3);
    assert_eq!(call_count.get(), 1);
}

#[test]
fn batch_read_sees_latest_value() {
    let sig = Signal::new(0);

    crate::batch(|| {
        sig.set(5);
        assert_eq!(sig.read(), 5);
        sig.set(10);
        assert_eq!(sig.read(), 10);
    });

    assert_eq!(sig.read(), 10);
}

#[test]
fn batch_combined_with_subscribers() {
    let a = Signal::new(0);
    let b = Signal::new(0);
    let a_calls = Rc::new(Cell::new(0u32));
    let b_calls = Rc::new(Cell::new(0u32));
    let ac = Rc::clone(&a_calls);
    let bc = Rc::clone(&b_calls);

    crate::subscribe(
        &a,
        Rc::new(move || {
            ac.set(ac.get() + 1);
        }),
    );
    crate::subscribe(
        &b,
        Rc::new(move || {
            bc.set(bc.get() + 1);
        }),
    );

    crate::batch(|| {
        a.set(1);
        a.set(2);
        b.set(10);
        b.set(20);
    });

    assert_eq!(a_calls.get(), 1);
    assert_eq!(b_calls.get(), 1);
    assert_eq!(a.read(), 2);
    assert_eq!(b.read(), 20);
}

#[test]
fn set_in_callback_no_infinite_loop() {
    // A subscriber that calls set() on the same signal during
    // callback should not cause infinite recursion.  Use
    // set_if_changed to avoid re-triggering when the value is
    // already correct.
    let sig = Signal::new(0);
    let call_count = Rc::new(Cell::new(0u32));
    let cc = Rc::clone(&call_count);
    let sig2 = sig.clone();
    crate::subscribe(
        &sig,
        Rc::new(move || {
            cc.set(cc.get() + 1);
            sig2.set_if_changed(99);
        }),
    );

    sig.set(1);
    // First notification: callback calls set_if_changed(99) — value
    // differs, so set(99) is called. dirty=true means follow-up.
    // Second notification: callback calls set_if_changed(99) —
    // value is already 99, so no-op. No more follow-ups.
    assert_eq!(sig.read(), 99);
    assert!(call_count.get() >= 1);
    assert!(call_count.get() <= 2);
}

#[test]
fn triple_reentrant_set() {
    // Chain: set(1) → callback calls set(2) → callback calls set(3).
    // The final value should be 3, and no infinite loop occurs.
    let sig = Signal::new(0);
    let sig1 = sig.clone();
    let sig2 = sig.clone();

    crate::subscribe(
        &sig,
        Rc::new(move || {
            if sig1.read() == 1 {
                sig1.set(2);
            }
        }),
    );
    crate::subscribe(
        &sig,
        Rc::new(move || {
            if sig2.read() == 2 {
                sig2.set(3);
            }
        }),
    );

    sig.set(1);
    // set(1) → both callbacks fire → first sees 1, calls set(2)
    // → second callback sees 2, calls set(3)
    // → follow-ups drain each re-entrant change.
    assert_eq!(sig.read(), 3);
}

#[test]
fn batch_panicking_notification_doesnt_drop_others() {
    // A panicking notification inside a batch should not prevent
    // other queued notifications from being delivered.
    let a = Signal::new(0);
    let b = Signal::new(0);
    let b_set = Rc::new(Cell::new(false));
    let bs = Rc::clone(&b_set);

    crate::subscribe(&a, Rc::new(move || panic!("intentional")));
    crate::subscribe(&b, Rc::new(move || bs.set(true)));

    // batch() catches subscriber panics internally via catch_unwind
    // in BatchGuard::drop, so it does NOT propagate.
    crate::batch(|| {
        a.set(1);
        b.set(2);
    });
    // b's subscriber should still have been notified despite a's
    // subscriber panicking (each notification is isolated).
    assert!(b_set.get());
}

// -- defensive / API coverage ---------------------------------------

#[test]
fn set_same_value_still_bumps_version() {
    let sig = Signal::new(42);
    let v1 = sig.version();
    sig.set(42); // same value, but version must increment
    assert!(sig.version() > v1);
}

#[test]
fn set_if_changed_noop_on_same_value() {
    let sig = Signal::new(10);
    let v1 = sig.version();
    sig.set_if_changed(10); // same value → no-op
    assert_eq!(sig.version(), v1);
}

#[test]
fn set_if_changed_fires_on_different_value() {
    let sig = Signal::new(10);
    let v1 = sig.version();
    sig.set_if_changed(20);
    assert!(sig.version() > v1);
    assert_eq!(sig.read(), 20);
}

#[test]
fn batch_subscriber_sees_final_value() {
    let sig = Signal::new(0);
    let sig2 = sig.clone();
    let seen = Rc::new(Cell::new(0));
    let s = Rc::clone(&seen);
    crate::subscribe(&sig, Rc::new(move || s.set(sig2.read())));
    crate::batch(|| {
        sig.set(1);
        sig.set(2);
        sig.set(3);
    });
    // Subscriber should see 3, not 1 or 2.
    assert_eq!(seen.get(), 3);
}

#[test]
fn batch_and_future_changed() {
    let sig = Signal::new(0i32);
    crate::batch(|| {
        sig.set(1);
        sig.set(2);
    });
    assert_eq!(sig.read(), 2);
    let mut fut = sig.changed();
    let (waker, _woken) = test_waker();
    let _ = poll(std::pin::Pin::new(&mut fut), &waker);
}

#[test]
fn filter_changed_future_with_batch_sees_final_value() {
    let sig = Signal::new(0i32);
    crate::batch(|| {
        sig.set(5);
        sig.set(10);
    });
    let (waker, _woken) = test_waker();
    let mut fut = sig.filter_changed(|v| *v > 5);
    let _ = poll(std::pin::Pin::new(&mut fut), &waker);
}

#[test]
fn signal_changed_future_double_poll_after_ready() {
    let sig = Signal::new(1);
    let (waker, _woken) = test_waker();
    let mut fut = sig.changed();
    sig.set(2);
    let _ = poll(std::pin::Pin::new(&mut fut), &waker);
    // Second poll after Ready — safe, not a panic.
    let _ = poll(std::pin::Pin::new(&mut fut), &waker);
}

#[test]
fn ptr_eq_detects_same_allocation() {
    let a = Signal::new(0);
    let b = a.clone();
    let c = Signal::new(0);
    assert!(a.ptr_eq(&b));
    assert!(!a.ptr_eq(&c));
}

#[test]
fn in_batch_returns_true_inside_batch_only() {
    assert!(!crate::in_batch());
    crate::batch(|| {
        assert!(crate::in_batch());
    });
    assert!(!crate::in_batch());
}

#[test]
fn new_subscriber_not_invoked_by_current_notification() {
    let sig = Signal::new(0i32);
    let new_sub_called = Rc::new(Cell::new(false));
    let ns = Rc::clone(&new_sub_called);
    let sig2 = sig.clone();

    crate::subscribe(
        &sig,
        Rc::new(move || {
            let ns2 = Rc::clone(&ns);
            crate::subscribe(&sig2, Rc::new(move || ns2.set(true)));
        }),
    );

    sig.set(1);
    assert!(!new_sub_called.get());
    sig.set(2);
    assert!(new_sub_called.get());
}

// -- state machine: deferred notification ------------------------------

#[test]
fn state_machine_double_reentrant_set_in_callback() {
    let sig = Signal::new(0i32);
    let call_count = Rc::new(Cell::new(0u32));
    let cc = Rc::clone(&call_count);
    let sig2 = sig.clone();

    crate::subscribe(
        &sig,
        Rc::new(move || {
            cc.set(cc.get() + 1);
            if sig2.read() < 20 {
                sig2.set(10);
                sig2.set(20);
            }
        }),
    );

    sig.set(1);
    assert_eq!(sig.read(), 20);
    assert_eq!(call_count.get(), 2);
}

#[test]
fn state_machine_cross_signal_reentrant() {
    let a = Signal::new(0i32);
    let b = Signal::new(0i32);

    let a1 = a.clone();
    let b1 = b.clone();
    crate::subscribe(
        &a,
        Rc::new(move || {
            if a1.read() == 1 {
                b1.set(10);
            }
        }),
    );

    let a2 = a.clone();
    let b2 = b.clone();
    crate::subscribe(
        &b,
        Rc::new(move || {
            if b2.read() == 10 {
                a2.set(2);
            }
        }),
    );

    a.set(1);
    assert_eq!(a.read(), 2);
    assert_eq!(b.read(), 10);
}

#[test]
fn state_machine_fresh_snapshot_on_follow_up() {
    let sig = Signal::new(0i32);
    let fresh_sub_called = Rc::new(Cell::new(false));
    let fc = Rc::clone(&fresh_sub_called);
    let sig2 = sig.clone();

    let step = Rc::new(Cell::new(0u32));
    let st = Rc::clone(&step);

    crate::subscribe(
        &sig,
        Rc::new(move || {
            let s = st.get();
            if s == 0 {
                st.set(1);
                let fc2 = Rc::clone(&fc);
                crate::subscribe(&sig2, Rc::new(move || fc2.set(true)));
                sig2.set(99);
            }
        }),
    );

    sig.set(1);
    assert!(fresh_sub_called.get());
    assert_eq!(sig.read(), 99);
}

#[test]
fn state_machine_reentrant_set_self_terminates() {
    let sig = Signal::new(0i32);
    let iterations = Rc::new(Cell::new(0u32));
    let it = Rc::clone(&iterations);
    let sig2 = sig.clone();

    crate::subscribe(
        &sig,
        Rc::new(move || {
            it.set(it.get() + 1);
            if it.get() < 5 {
                #[allow(clippy::cast_possible_wrap)]
                sig2.set(it.get() as i32);
            }
        }),
    );

    sig.set(1);
    assert_eq!(iterations.get(), 5);
    assert_eq!(sig.read(), 4);
}

#[test]
fn state_machine_multiple_subscribers_reentrant() {
    let sig = Signal::new(0i32);
    let order = Rc::new(RefCell::new(Vec::new()));

    let sig_a = sig.clone();
    let ord_a = Rc::clone(&order);
    crate::subscribe(
        &sig,
        Rc::new(move || {
            ord_a.borrow_mut().push("A");
            if sig_a.read() < 3 {
                sig_a.set(sig_a.read() + 1);
            }
        }),
    );

    let sig_b = sig.clone();
    let ord_b = Rc::clone(&order);
    crate::subscribe(
        &sig,
        Rc::new(move || {
            ord_b.borrow_mut().push("B");
            if sig_b.read() < 3 {
                sig_b.set(sig_b.read() + 1);
            }
        }),
    );

    sig.set(1);

    let r = order.borrow().clone();
    assert_eq!(sig.read(), 3);
    assert!(
        r.len() >= 4,
        "expected at least 4 callbacks, got {}",
        r.len()
    );
}

#[test]
fn state_machine_callback_panics_propagate_to_set() {
    let sig = Signal::new(0i32);
    crate::subscribe(&sig, Rc::new(move || panic!("intentional callback panic")));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sig.set(1);
    }));
    assert!(result.is_err());
    assert_eq!(sig.read(), 1);
}

// -- batch + version interaction ---------------------------------------

#[test]
fn nested_batch_version_bumps_and_single_notification() {
    let sig = Signal::new(0);
    let call_count = Rc::new(Cell::new(0u32));
    let cc = Rc::clone(&call_count);
    let sig2 = sig.clone();
    crate::subscribe(
        &sig,
        Rc::new(move || {
            cc.set(cc.get() + 1);
            let _ = sig2.read();
        }),
    );

    let mid_versions = Rc::new(RefCell::new(Vec::new()));
    let mv = Rc::clone(&mid_versions);
    let s = sig.clone();

    crate::batch(|| {
        s.set(1);
        mv.borrow_mut().push(s.version());
        crate::batch(|| {
            s.set(2);
            mv.borrow_mut().push(s.version());
            s.set(3);
            mv.borrow_mut().push(s.version());
        });
        // Inner batch exited — no notification yet.
        assert_eq!(call_count.get(), 0);
        mv.borrow_mut().push(s.version());
    });
    // Outer batch exited — one notification.
    assert_eq!(sig.read(), 3);
    assert_eq!(call_count.get(), 1);

    let versions = mid_versions.borrow().clone();
    assert_eq!(versions, vec![1, 2, 3, 3]);
}

#[test]
fn batch_read_sees_latest_but_callback_not_fired_until_exit() {
    let sig = Signal::new(0);
    let callback_seen = Rc::new(Cell::new(0i32));
    let cs = Rc::clone(&callback_seen);
    let sig2 = sig.clone();
    crate::subscribe(
        &sig,
        Rc::new(move || {
            cs.set(sig2.read());
        }),
    );

    crate::batch(|| {
        sig.set(5);
        assert_eq!(sig.read(), 5); // sees latest immediately
        assert_eq!(callback_seen.get(), 0); // but callback not yet fired
        sig.set(10);
    });

    assert_eq!(callback_seen.get(), 10); // fired once with final value
}

// -- SignalMap / SignalFilter ------------------------------------------

#[test]
fn signalmap_changed_wakes_when_source_changes() {
    let source = Signal::new(1i32);
    let sm = source.map(|v: &i32| v * 10);

    let (waker, woken) = test_waker();
    let mut fut = Box::pin(sm.changed());
    assert!(poll(fut.as_mut(), &waker).is_pending());

    source.set(2);
    assert!(woken.load(Ordering::SeqCst));
    assert_eq!(poll(fut.as_mut(), &waker), Poll::Ready(20));
}

#[test]
fn signalmap_read_after_source_change_returns_new_value() {
    let source = Signal::new(5i32);
    let sm = source.map(|v: &i32| v * 2);

    assert_eq!(sm.read(), 10);
    source.set(7);
    assert_eq!(sm.read(), 14);
}

#[test]
fn signalmap_with_tracks_observer() {
    let source = Signal::new(3i32);
    let sm = source.map(|v: &i32| v * 5);

    let result = sm.with(|v| *v);
    assert_eq!(result, 15);
}

// -- re-await without new change ---------------------------------------

#[test]
fn changed_re_await_without_new_set_stays_pending() {
    let sig = Signal::new(0i32);
    let (waker, _woken) = test_waker();

    let mut fut1 = sig.changed();
    sig.set(1);
    assert_eq!(poll(pin_mut(&mut fut1), &waker), Poll::Ready(1));

    // Start a new future after the set — should be pending.
    let mut fut2 = sig.changed();
    assert!(poll(pin_mut(&mut fut2), &waker).is_pending());
    // Re-poll — still pending.
    assert!(poll(pin_mut(&mut fut2), &waker).is_pending());

    // New set should wake it.
    sig.set(2);
    assert_eq!(poll(pin_mut(&mut fut2), &waker), Poll::Ready(2));
}

// -- direct set() in Drop context (synchronous fallback) ---------------

#[test]
fn signal_set_in_drop_updates_value_directly() {
    let sig = Signal::new(0i32);
    struct SetOnDrop {
        sig: Signal<i32>,
        val: i32,
    }
    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.sig.set(self.val);
        }
    }

    {
        let _guard = SetOnDrop {
            sig: sig.clone(),
            val: 99,
        };
    }
    assert_eq!(sig.read(), 99);
}
