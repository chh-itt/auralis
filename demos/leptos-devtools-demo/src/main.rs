//! Leptos DevTools Demo.
//! Run: `cd demos/leptos-devtools-demo && trunk serve`

use leptos::mount::mount_to_body;
use leptos::prelude::*;
use wasm_bindgen::prelude::*;

mod app;
#[macro_use]
mod mirror;

use app::App;

fn main() {
    console_error_panic_hook::set_once();

    // Install WASM-compatible timing for Memo recompute profiling.
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

    mount_to_body(|| view! { <App/> });
    init_devtools_bridge();
}

fn init_devtools_bridge() {
    use std::cell::Cell;
    use std::rc::Rc;

    let open: Rc<Cell<bool>> = Rc::new(Cell::new(false));
    let force_render: Rc<Cell<bool>> = Rc::new(Cell::new(false));

    // Toggle: flips the flag AND toggles the DOM panel visibility.
    let open2 = open.clone();
    let force_render_toggle = force_render.clone();
    let toggle = Closure::<dyn FnMut()>::new(Box::new(move || {
        let v = !open2.get();
        open2.set(v);
        if v {
            // Force an immediate render when the panel opens.
            force_render_toggle.set(true);
            auralis_signal::mark_changed();
        }
        // Toggle the DOM panel via proper DOM interop.
        if let Ok(document) = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("document")) {
            if let Ok(get_by_id) = js_sys::Reflect::get(&document, &JsValue::from_str("getElementById")) {
                let get_by_id: js_sys::Function = get_by_id.unchecked_into();
                if let Ok(el) = get_by_id.call1(&document, &JsValue::from_str("devtools")) {
                    let cur = js_sys::Reflect::get(&el, &JsValue::from_str("className"))
                        .ok()
                        .and_then(|c| c.as_string())
                        .unwrap_or_default();
                    let next = if cur == "open" { "" } else { "open" };
                    let _ = js_sys::Reflect::set(&el, &JsValue::from_str("className"), &JsValue::from_str(next));
                }
            }
        }
    }));
    js_sys::Reflect::set(
        &js_sys::global(),
        &JsValue::from_str("_toggleDevtools"),
        toggle.as_ref().unchecked_ref(),
    )
    .ok();
    toggle.forget();

    // Poll callback: serializes a snapshot and pushes it to _dtRender
    // via proper JS interop, avoiding eval-based string interpolation.
    // Skips the expensive snapshot+render when the reactive graph is idle.
    let open3 = open;
    let force_render_poll = force_render;
    let poll = Closure::<dyn FnMut()>::new(Box::new(move || {
        if !open3.get() {
            return;
        }
        // Fast path: no mutations since last poll AND not the first
        // render after opening the panel.
        if !force_render_poll.replace(false) && !auralis_signal::take_changed_flag() {
            return;
        }
        let snap = auralis_devtools::snapshot();
        let json_str = serde_json::to_string(&snap).unwrap();
        if let Ok(js_value) = js_sys::JSON::parse(&json_str) {
            let dt_render: js_sys::Function = js_sys::Reflect::get(
                &js_sys::global(),
                &JsValue::from_str("_dtRender"),
            )
            .unwrap()
            .unchecked_into();
            let _ = dt_render.call1(&JsValue::NULL, &js_value);
        }
    }));
    js_sys::Reflect::set(
        &js_sys::global(),
        &JsValue::from_str("_dtPollCb"),
        poll.as_ref().unchecked_ref(),
    )
    .ok();
    poll.forget();

    // Start polling at ~6.6 Hz after a 100ms initial delay.
    let start_poll = Closure::<dyn FnMut()>::new(Box::new(move || {
        if let Ok(poll_cb) = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("_dtPollCb")) {
            let set_interval: js_sys::Function = js_sys::Reflect::get(
                &js_sys::global(),
                &JsValue::from_str("setInterval"),
            )
            .unwrap()
            .unchecked_into();
            let _ = set_interval.call2(&JsValue::NULL, &poll_cb, &JsValue::from_f64(150.0));
        }
    }));
    let set_timeout: js_sys::Function = js_sys::Reflect::get(
        &js_sys::global(),
        &JsValue::from_str("setTimeout"),
    )
    .unwrap()
    .unchecked_into();
    let _ = set_timeout.call2(&JsValue::NULL, start_poll.as_ref(), &JsValue::from_f64(100.0));
    start_poll.forget();
}
