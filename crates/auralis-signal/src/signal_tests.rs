//! Basic tests for `auralis_signal::Signal` — version tracking on `set()`.
//! Most signal behaviour is tested via the futures and batch tests in
//! `future_tests.rs`.

use super::*;

#[test]
fn signal_version_increments_on_set() {
    let sig = Signal::new(0i32);
    assert_eq!(sig.version(), 0);

    sig.set(1);
    assert_eq!(sig.version(), 1);

    sig.set(2);
    assert_eq!(sig.version(), 2);
}

#[test]
fn signal_version_unchanged_without_set() {
    let sig = Signal::new(42);
    let v1 = sig.version();
    // read() does not change version.
    let _ = sig.read();
    assert_eq!(sig.version(), v1);
}

#[test]
fn signal_label_set_and_get() {
    let sig = Signal::new(0);
    assert_eq!(sig.label(), None);

    sig.set_label("counter");
    assert_eq!(sig.label(), Some("counter".to_string()));

    // Overwriting works.
    sig.set_label("renamed");
    assert_eq!(sig.label(), Some("renamed".to_string()));
}

#[test]
fn signal_label_clone_shares_label() {
    let sig = Signal::new(0);
    sig.set_label("shared");
    let clone = sig.clone();
    assert_eq!(clone.label(), Some("shared".to_string()));

    // Mutating through the clone affects the original.
    clone.set_label("updated");
    assert_eq!(sig.label(), Some("updated".to_string()));
}

#[test]
fn add_and_remove_schedule_observer() {
    use std::cell::Cell;
    use std::rc::Rc;

    let call_count = Rc::new(Cell::new(0));
    let cc = call_count.clone();
    let token = add_schedule_observer(Box::new(move || {
        cc.set(cc.get() + 1);
    }));

    // Install a primary hook so signals are routed (not sync fallback).
    install_schedule_hook(Box::new(|cb| cb()));

    let sig = Signal::new(0);
    sig.set(1);
    sig.set(2);

    assert_eq!(call_count.get(), 2);

    remove_schedule_observer(token);
    sig.set(3);
    // Observer was removed — count should not increase.
    assert_eq!(call_count.get(), 2);

    remove_schedule_hook();
}

#[test]
fn remove_schedule_observer_wrong_token_is_noop() {
    let token = add_schedule_observer(Box::new(|| {}));
    remove_schedule_observer(token);
    // Second removal with the same token is a no-op (no panic).
    remove_schedule_observer(token);
}

#[test]
fn observer_token_generation_prevents_stale_removal() {
    use std::cell::Cell;
    use std::rc::Rc;

    install_schedule_hook(Box::new(|cb| cb()));

    let call_count = Rc::new(Cell::new(0));
    let cc = call_count.clone();
    let token_a = add_schedule_observer(Box::new(move || {
        cc.set(cc.get() + 1);
    }));

    // Verify token_a works.
    let sig = Signal::new(0);
    sig.set(1);
    assert_eq!(call_count.get(), 1);

    // Remove observer A. This bumps the generation on slot 0.
    remove_schedule_observer(token_a);

    // Add observer B — it should reuse slot 0 with gen=1.
    let cc2 = Rc::new(Cell::new(0));
    let c_ref = cc2.clone();
    let token_b = add_schedule_observer(Box::new(move || {
        c_ref.set(c_ref.get() + 1);
    }));

    // Stale token should NOT remove the new observer.
    remove_schedule_observer(token_a);

    sig.set(2);
    assert_eq!(
        call_count.get(),
        1,
        "stale token should not affect observer A (already removed)"
    );
    assert_eq!(cc2.get(), 1, "observer B should still fire");

    // Fresh token works.
    remove_schedule_observer(token_b);

    remove_schedule_hook();
}

#[test]
fn observer_reentrant_set_does_not_panic() {
    install_schedule_hook(Box::new(|cb| cb()));

    // Two independent signals (not clones — different Rc allocations).
    let sig_a = Signal::new(0);
    let sig_b = Signal::new(0);
    let b = sig_b.clone();
    let token = add_schedule_observer(Box::new(move || {
        b.set(b.read() + 1);
    }));

    // This triggers the observer on sig_a. The observer calls set on
    // sig_b, which calls notify_schedule_observers again. The
    // re-entrancy guard prevents a NOTIFY_OBSERVERS RefCell panic.
    sig_a.set(1);

    // sig_b was incremented once by the observer.
    assert_eq!(sig_b.version(), 1);

    remove_schedule_observer(token);
    remove_schedule_hook();
}

#[test]
fn panicking_observer_does_not_block_others() {
    use std::cell::Cell;
    use std::rc::Rc;

    install_schedule_hook(Box::new(|cb| cb()));

    let call_count = Rc::new(Cell::new(0));
    let cc = call_count.clone();
    let _panic_token = add_schedule_observer(Box::new(|| {
        panic!("observer 1 panics");
    }));
    let _token2 = add_schedule_observer(Box::new(move || {
        cc.set(cc.get() + 1);
    }));

    // Observer 1 panics, but observer 2 should still fire.
    let sig = Signal::new(0);
    sig.set(1);

    assert_eq!(
        call_count.get(),
        1,
        "observer 2 should fire despite observer 1 panic"
    );

    // Clean up.
    NOTIFY_OBSERVERS.with(|cell| cell.borrow_mut().clear());
    remove_schedule_hook();
}
