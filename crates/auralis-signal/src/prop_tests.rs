//! Tests for prop module.

use crate::prop::{IntoProp, Prop, StaticProp};
use crate::Signal;

#[test]
fn static_prop_resolve() {
    let p = StaticProp(42i32);
    assert_eq!(p.resolve(), 42);
}

#[test]
fn static_prop_subscribe_returns_none() {
    let p = StaticProp("hello".to_string());
    assert!(p.subscribe(Box::new(|| {})).is_none());
}

#[test]
fn signal_prop_resolve() {
    crate::install_schedule_hook(Box::new(|f| f()));
    let sig = Signal::new(100u32);
    let p: Signal<u32> = sig.into_prop();
    assert_eq!(p.resolve(), 100);
}

#[test]
fn signal_prop_subscribe_fires() {
    crate::install_schedule_hook(Box::new(|f| f()));
    let sig = Signal::new(0u32);
    let called = std::rc::Rc::new(std::cell::Cell::new(false));
    let c = called.clone();
    let handle = sig.subscribe(Box::new(move || c.set(true)));
    assert!(handle.is_some());
    sig.set(1);
    assert!(called.get());
}

#[test]
fn into_prop_from_t() {
    let p: StaticProp<i32> = 42i32.into_prop();
    assert_eq!(p.resolve(), 42);
}

#[test]
fn into_prop_from_signal_annotated() {
    crate::install_schedule_hook(Box::new(|f| f()));
    let sig = Signal::new("dynamic".to_string());
    let p: Signal<String> = sig.into_prop();
    assert_eq!(p.resolve(), "dynamic");
}

#[test]
fn into_prop_from_str_annotated() {
    let p: StaticProp<String> = <&str as IntoProp<String>>::into_prop("hello");
    assert_eq!(p.resolve(), "hello".to_string());
}

#[test]
fn prop_tracks_changes() {
    crate::install_schedule_hook(Box::new(|f| f()));
    let sig = Signal::new(0u32);
    let p: Signal<u32> = sig.clone().into_prop();
    assert_eq!(p.resolve(), 0);
    sig.set(42);
    assert_eq!(p.resolve(), 42);
}
