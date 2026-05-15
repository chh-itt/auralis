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
    mount_to_body(|| view! { <App/> });
    init_devtools_bridge();
}

fn init_devtools_bridge() {
    use std::cell::Cell;
    use std::rc::Rc;

    let open: Rc<Cell<bool>> = Rc::new(Cell::new(false));

    // Toggle: flips the flag AND toggles the DOM panel visibility.
    let open2 = open.clone();
    let toggle = Closure::<dyn FnMut()>::new(Box::new(move || {
        let v = !open2.get();
        open2.set(v);
        // Toggle the DOM panel via JS.
        let _ = js_sys::eval(
            "var d=document.getElementById('devtools');d.className=d.className==='open'?'':'open';"
        );
    }));
    js_sys::Reflect::set(
        &js_sys::global(),
        &JsValue::from_str("_toggleDevtools"),
        toggle.as_ref().unchecked_ref(),
    )
    .ok();
    toggle.forget();

    // Poll callback: pushes JSON to _dtRender every time it's called.
    let open3 = open;
    let poll = Closure::<dyn FnMut()>::new(Box::new(move || {
        if !open3.get() {
            return;
        }
        let snap = auralis_devtools::snapshot();
        if let Ok(json) = serde_json::to_string_pretty(&snap) {
            let escaped = json.replace('\\', "\\\\").replace('\'', "\\'").replace('\n', "\\n");
            let _ = js_sys::eval(&format!("_dtRender('{escaped}')"));
        }
    }));
    js_sys::Reflect::set(
        &js_sys::global(),
        &JsValue::from_str("_dtPollCb"),
        poll.as_ref().unchecked_ref(),
    )
    .ok();
    poll.forget();

    // Poll at ~6.6 Hz — tight enough for near-real-time feedback,
    // slow enough to keep the D3 graph simulation stable.
    let _ = js_sys::eval("setTimeout(function(){setInterval(function(){_dtPollCb();},150)}, 100)");
}
