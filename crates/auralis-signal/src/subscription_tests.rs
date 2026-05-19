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
