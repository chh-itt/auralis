//! WASM bridge — one-line setup for in-browser `DevTools`.
//!
//! ```rust,ignore
//! use auralis_devtools::wasm;
//!
//! wasm::install_timing_hook();
//! let _poll = wasm::start_polling(150, || true);
//! ```

use std::rc::Rc;

use wasm_bindgen::prelude::*;

// ---------------------------------------------------------------------------
// Timing hook
// ---------------------------------------------------------------------------

/// Install a WASM-compatible timing hook so that [`Memo`] recompute
/// profiling works in the browser.
///
/// Uses `performance.now()` (microsecond resolution).  No-op if called
/// more than once (the first installed hook wins).
///
/// [`Memo`]: auralis_signal::Memo
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn install_timing_hook() {
    auralis_signal::install_timing_hook(|| {
        let ms = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("performance"))
            .ok()
            .and_then(|perf| {
                let now: js_sys::Function = js_sys::Reflect::get(&perf, &JsValue::from_str("now"))
                    .ok()?
                    .unchecked_into();
                now.call0(&perf).ok()?.as_f64()
            })
            .unwrap_or(0.0);
        (ms * 1000.0) as u64 // ms → µs
    });
}

// ---------------------------------------------------------------------------
// Snapshot push
// ---------------------------------------------------------------------------

/// Take a snapshot and push it to the JS `_dtRender` callback.
///
/// The snapshot is serialised to JSON, parsed back into a JS value
/// (to avoid string-escaping issues), and passed to `window._dtRender`.
/// Does nothing if `_dtRender` is not defined.
pub fn push_snapshot() {
    let snap = crate::snapshot();
    let Ok(json_str) = serde_json::to_string(&snap) else {
        return;
    };
    let Ok(js_value) = js_sys::JSON::parse(&json_str) else {
        return;
    };

    let Ok(dt_render) = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("_dtRender"))
    else {
        return;
    };
    let render_fn: js_sys::Function = dt_render.unchecked_into();
    let _ = render_fn.call1(&JsValue::NULL, &js_value);
}

// ---------------------------------------------------------------------------
// Periodic polling
// ---------------------------------------------------------------------------

/// Handle returned by [`start_polling`].  Drop it to stop polling.
#[must_use]
pub struct PollHandle {
    interval_id: i32,
    _closure: Closure<dyn FnMut()>,
}

impl Drop for PollHandle {
    fn drop(&mut self) {
        if let Ok(clear_fn) =
            js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("clearInterval"))
        {
            let clear: js_sys::Function = clear_fn.unchecked_into();
            let _ = clear.call1(
                &JsValue::NULL,
                &JsValue::from_f64(f64::from(self.interval_id)),
            );
        }
    }
}

/// Start periodic polling at `interval_ms` milliseconds.
///
/// Before each snapshot, `is_open` is called; when it returns `false`
/// the poll is skipped entirely, saving CPU when the `DevTools` panel is
/// closed.  When open, the idle flag is checked via
/// [`auralis_signal::take_changed_flag`] — if no signals have changed
/// since the last poll the snapshot is also skipped.
///
/// Returns a [`PollHandle`] that stops the interval when dropped.
///
/// # Panics
///
/// Panics if `setInterval` is not available in the JS runtime.  This
/// should never happen in a browser environment.
#[allow(clippy::cast_possible_truncation)]
pub fn start_polling(interval_ms: u32, is_open: impl Fn() -> bool + 'static) -> PollHandle {
    let is_open = Rc::new(is_open);
    let is_open2 = Rc::clone(&is_open);

    let closure = Closure::new(move || {
        if !is_open2() {
            return;
        }
        // Fast path: skip when no mutations occurred.
        if !auralis_signal::take_changed_flag() {
            return;
        }
        push_snapshot();
    });

    let js_func = closure.as_ref().unchecked_ref::<js_sys::Function>();

    let set_interval: js_sys::Function =
        js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("setInterval"))
            .expect("setInterval not found")
            .unchecked_into();

    let result: f64 = set_interval
        .call2(
            &JsValue::NULL,
            js_func,
            &JsValue::from_f64(f64::from(interval_ms)),
        )
        .expect("setInterval failed")
        .as_f64()
        .expect("setInterval returned non-number");

    PollHandle {
        interval_id: result as i32,
        _closure: closure,
    }
}
