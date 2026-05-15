//! Lightweight bridge: mirror a Leptos signal into Auralis for
//! real-time DevTools monitoring.
//!
//! Uses only public Leptos APIs (`Get::get`, `Effect::new`) and
//! Auralis APIs (`Signal::new`, `set_label`, `Signal::set`).
//!
//! # Usage (inside a Leptos component)
//!
//! ```ignore
//! let (count, set_count) = leptos::prelude::signal(0);
//! let mirror_count = mirror!(count, "counter");
//! // mirror_count is an auralis_signal::Signal<i32>
//! ```

/// Create an Auralis [`Signal`](auralis_signal::Signal) that mirrors a
/// Leptos signal.  The initial value is copied from the Leptos signal,
/// and a `leptos::prelude::Effect::new` keeps them in sync.
#[macro_export]
macro_rules! mirror {
    ($leptos_sig:expr, $label:expr) => {{
        let ls = $leptos_sig;
        let m = auralis_signal::Signal::new(ls.get_untracked());
        m.set_label($label);
        let m2 = m.clone();
        leptos::prelude::Effect::new(move || {
            m2.set(ls.get());
        });
        m
    }};
}

/// Mirror a Leptos [`Memo`](leptos::prelude::Memo) into an Auralis
/// signal.  The derivation lives in Leptos; the Auralis side sees
/// it as a plain signal updated via `Effect::new`.
#[macro_export]
macro_rules! mirror_memo {
    ($leptos_memo:expr, $label:expr) => {{
        let lm = $leptos_memo;
        let s = auralis_signal::Signal::new(lm.get_untracked());
        s.set_label($label);
        let s2 = s.clone();
        leptos::prelude::Effect::new(move || {
            s2.set(lm.get());
        });
        s
    }};
}

/// Mirror a Leptos memo into an Auralis **Memo** that reads a
/// single dependency mirror, so DevTools can render the dependency
/// edge (signal → memo arrow).
///
/// For multiple dependencies, call this macro once per dep and
/// create intermediate Auralis memos to chain them.
///
/// # Example
///
/// ```ignore
/// let mir_items = mirror!(items, "items");
/// let mir_total = mirror_memo_deps!(
///     total_count, "total_count", mir_items
/// );
/// ```
#[macro_export]
macro_rules! mirror_memo_deps {
    ($leptos_memo:expr, $label:expr, $dep:expr) => {{
        let lm = $leptos_memo;
        let dep = $dep.clone();
        let a_memo = auralis_signal::Memo::new(move || {
            let _ = dep.read();
            lm.get_untracked()
        });
        a_memo.set_label($label);
        let am = a_memo.clone();
        leptos::prelude::Effect::new(move || {
            let _ = lm.get();
            let _ = am.read();
        });
        a_memo
    }};
}
