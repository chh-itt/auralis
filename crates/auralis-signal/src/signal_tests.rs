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
