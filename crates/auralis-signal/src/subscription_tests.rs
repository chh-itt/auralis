//! Tests for subscription module.

use crate::subscription::subscribe_to;
use crate::Signal;

#[test]
fn subscribe_to_fires_on_mutation() {
    crate::install_schedule_hook(Box::new(|f| f()));
    let sig = Signal::new(0);
    let called = std::rc::Rc::new(std::cell::Cell::new(false));
    let c = called.clone();
    let _handle = subscribe_to(&sig, move || c.set(true));
    sig.set(1);
    assert!(called.get());
}

#[test]
fn unsubscribe_on_drop() {
    crate::install_schedule_hook(Box::new(|f| f()));
    let sig = Signal::new(0);
    let count = std::rc::Rc::new(std::cell::Cell::new(0u32));
    let c = count.clone();
    let handle = subscribe_to(&sig, move || {
        c.set(c.get() + 1);
    });
    sig.set(1);
    assert_eq!(count.get(), 1);
    drop(handle);
    sig.set(2);
    assert_eq!(count.get(), 1); // no longer subscribed
}

#[test]
fn multiple_handles_independent() {
    crate::install_schedule_hook(Box::new(|f| f()));
    let sig = Signal::new(0);
    let a = std::rc::Rc::new(std::cell::Cell::new(0u32));
    let b = std::rc::Rc::new(std::cell::Cell::new(0u32));
    let a1 = a.clone();
    let b1 = b.clone();
    let h1 = subscribe_to(&sig, move || a1.set(a1.get() + 1));
    let h2 = subscribe_to(&sig, move || b1.set(b1.get() + 1));
    sig.set(1);
    assert_eq!(a.get(), 1);
    assert_eq!(b.get(), 1);
    drop(h1);
    sig.set(2);
    assert_eq!(a.get(), 1); // h1 unsubscribed
    assert_eq!(b.get(), 2); // h2 still active
    drop(h2);
}

#[test]
fn debug_format() {
    let sig = Signal::new(0);
    let handle = subscribe_to(&sig, || {});
    assert!(format!("{handle:?}").contains("SubscriptionHandle"));
    drop(handle);
}

// ── Weak derived subscriptions (self-unsubscribing) ────────────────────

use crate::subscription::subscribe_derived;

#[test]
fn derived_subscription_updates_target_while_alive() {
    crate::install_schedule_hook(Box::new(|f| f()));
    let source = Signal::new(1);
    let target = Signal::new(String::new());
    subscribe_derived(&source, &target, |t| t.set("changed".to_string()));
    source.set(2);
    assert_eq!(target.read(), "changed");
}

#[test]
fn derived_subscription_self_unsubscribes_when_target_dropped() {
    crate::install_schedule_hook(Box::new(|f| f()));
    let source = Signal::new(1);
    let target = Signal::new(0u32);
    subscribe_derived(&source, &target, |t| t.set(t.read_untracked() + 1));
    assert_eq!(source.subscriber_count(), 1);

    source.set(2);
    assert_eq!(target.read(), 1);

    drop(target);
    // First notification after target death: callback sees a dead weak,
    // unsubscribes itself.
    source.set(3);
    assert_eq!(source.subscriber_count(), 0);
    // Subsequent sets stay at zero (no ghost callbacks).
    source.set(4);
    assert_eq!(source.subscriber_count(), 0);
}

#[test]
fn derived_subscription_does_not_keep_target_alive() {
    crate::install_schedule_hook(Box::new(|f| f()));
    let source = Signal::new(1);
    let target = Signal::new(0u32);
    let weak_probe = target.downgrade();
    subscribe_derived(&source, &target, |t| t.set(t.read_untracked() + 1));
    drop(target);
    // The subscription must hold only a weak ref — target must be dead now.
    assert!(weak_probe.upgrade().is_none());
}

#[test]
fn derived_subscription_shared_target_stays_alive_until_last_clone() {
    crate::install_schedule_hook(Box::new(|f| f()));
    let source = Signal::new(1);
    let target = Signal::new(0u32);
    let second_owner = target.clone();
    subscribe_derived(&source, &target, |t| t.set(t.read_untracked() + 1));

    drop(target);
    // A clone still owns the target — updates must continue.
    source.set(2);
    assert_eq!(second_owner.read(), 1);
    assert_eq!(source.subscriber_count(), 1);

    drop(second_owner);
    source.set(3);
    assert_eq!(source.subscriber_count(), 0);
}

#[test]
fn weak_signal_upgrade_roundtrip() {
    let sig = Signal::new(42);
    let weak = sig.downgrade();
    let strong = weak.upgrade().expect("signal alive");
    assert_eq!(strong.read_untracked(), 42);
    drop(strong);
    drop(sig);
    assert!(weak.upgrade().is_none());
}
