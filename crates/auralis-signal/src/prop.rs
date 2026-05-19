//! `Prop<T>` — unified static / dynamic value trait.
//!
//! Bridges plain values (`T`) and reactive values ([`Signal<T>`]) under
//! a single interface.  Widget constructors accept `impl IntoProp<T>`
//! and call [`Prop::resolve`] / [`Prop::subscribe`] uniformly,
//! eliminating the need for `Static` / `Dynamic` enum forks.

use crate::subscription::SubscriptionHandle;
use crate::Signal;

/// A property that may be static or driven by a [`Signal`].
///
/// # Type parameters
///
/// * `T: Clone + 'static` — the value type, e.g. `String`, `f32`, `bool`.
pub trait Prop<T: Clone + 'static> {
    /// Return the current value.
    fn resolve(&self) -> T;

    /// Subscribe to changes, returning a [`SubscriptionHandle`] that
    /// unsubscribes on drop.  Returns `None` for static values that
    /// never change.
    fn subscribe(&self, on_change: Box<dyn Fn()>) -> Option<SubscriptionHandle>;
}

/// Convenience conversion trait — `impl IntoProp<T>` accepts both
/// `T` (static) and `Signal<T>` (dynamic).
pub trait IntoProp<T: Clone + 'static> {
    /// The concrete [`Prop`] type produced by this conversion.
    type PropType: Prop<T> + 'static;

    /// Convert `self` into a [`Prop`] value.
    fn into_prop(self) -> Self::PropType;
}

// ---------------------------------------------------------------------------
// StaticProp — wraps a plain value
// ---------------------------------------------------------------------------

/// Wrapper that adapts a plain value for the [`Prop`] trait.
#[derive(Debug, Clone)]
pub struct StaticProp<T: Clone + 'static>(pub T);

impl<T: Clone + 'static> Prop<T> for StaticProp<T> {
    fn resolve(&self) -> T {
        self.0.clone()
    }

    fn subscribe(&self, _on_change: Box<dyn Fn()>) -> Option<SubscriptionHandle> {
        // Static values never change — no subscription needed.
        None
    }
}

// Blanket conversion for plain values — implemented for common types via a
// macro to avoid conflicting with the `Signal<T>` impl (both match `T: Clone`).
macro_rules! impl_into_prop_static {
    ($($T:ty),+ $(,)?) => { $(
        impl IntoProp<$T> for $T {
            type PropType = StaticProp<$T>;
            fn into_prop(self) -> Self::PropType { StaticProp(self) }
        }
    )+ };
}

impl_into_prop_static!(String, f32, f64, i32, u32, i64, u64, bool, usize);

// -- impl for Signal<T> ------------------------------------------------------

impl<T: Clone + 'static> Prop<T> for Signal<T> {
    fn resolve(&self) -> T {
        Signal::read(self)
    }

    fn subscribe(&self, on_change: Box<dyn Fn()>) -> Option<SubscriptionHandle> {
        Some(crate::subscription::subscribe_to_dyn(self, on_change))
    }
}

impl<T: Clone + 'static> IntoProp<T> for Signal<T> {
    type PropType = Signal<T>;

    fn into_prop(self) -> Self::PropType {
        self
    }
}

// -- impl for &str → StaticProp<String> --------------------------------------

impl IntoProp<String> for &str {
    type PropType = StaticProp<String>;

    fn into_prop(self) -> Self::PropType {
        StaticProp(self.to_string())
    }
}

#[cfg(test)]
#[path = "prop_tests.rs"]
mod tests;
