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

    // One line: install WASM-compatible timing for Memo recompute profiling.
    auralis_devtools::wasm::install_timing_hook();

    mount_to_body(|| view! { <App/> });
    init_devtools_bridge();
}

fn init_devtools_bridge() {
    use std::cell::Cell;
    use std::rc::Rc;

    let open: Rc<Cell<bool>> = Rc::new(Cell::new(false));
    let force_toggle: Rc<Cell<bool>> = Rc::new(Cell::new(false));

    // Toggle button: flips the flag and toggles the DOM panel visibility.
    let open_for_toggle = open.clone();
    let force_toggle_clone = force_toggle.clone();
    let toggle = Closure::<dyn FnMut()>::new(Box::new(move || {
        let v = !open_for_toggle.get();
        open_for_toggle.set(v);
        if v {
            force_toggle_clone.set(true);
            auralis_signal::mark_changed();
        }
        // Toggle the DOM panel via proper DOM interop.
        if let Ok(document) =
            js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("document"))
        {
            if let Ok(get_by_id) =
                js_sys::Reflect::get(&document, &JsValue::from_str("getElementById"))
            {
                let get_by_id: js_sys::Function = get_by_id.unchecked_into();
                if let Ok(el) = get_by_id.call1(&document, &JsValue::from_str("devtools")) {
                    let cur = js_sys::Reflect::get(&el, &JsValue::from_str("className"))
                        .ok()
                        .and_then(|c| c.as_string())
                        .unwrap_or_default();
                    let next = if cur == "open" { "" } else { "open" };
                    let _ = js_sys::Reflect::set(
                        &el,
                        &JsValue::from_str("className"),
                        &JsValue::from_str(next),
                    );
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

    // Periodic snapshot polling via the built-in WASM bridge.
    // The is_open callback skips work when the panel is closed.
    let force_toggle_for_poll = force_toggle;
    let open_for_poll = open;
    let _poll = auralis_devtools::wasm::start_polling(150, move || {
        // Force an immediate snapshot after the panel opens.
        if force_toggle_for_poll.replace(false) {
            return true;
        }
        open_for_poll.get()
    });
    // PollHandle is leaked intentionally — the interval runs for the
    // lifetime of the page.
    std::mem::forget(_poll);
}
