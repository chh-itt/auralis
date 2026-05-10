use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use auralis_signal::Signal;
use auralis_task::{timer, Executor, TaskScope, TimeSource};
use wasm_bindgen::prelude::*;
use web_sys::window;

// ---- DOM helpers ----

fn set_text(id: &str, text: &str) {
    if let Some(el) = window().and_then(|w| w.document()).and_then(|d| d.get_element_by_id(id)) {
        el.set_text_content(Some(text));
    }
}

fn button(id: &str, f: impl FnMut() + 'static) {
    let el = window()
        .and_then(|w| w.document())
        .and_then(|d| d.get_element_by_id(id))
        .unwrap_or_else(|| panic!("element not found: {id}"));
    let closure = Closure::wrap(Box::new(f) as Box<dyn FnMut()>);
    let _ = el.add_event_listener_with_callback("click", closure.as_ref().unchecked_ref());
    closure.forget();
}

// ---- entry ----

#[wasm_bindgen(start)]
pub fn main() {
    let count = Signal::new(0i32);

    // ---- executor + TimeSource ----
    let ex = Executor::new_instance();

    struct WasmClock;
    impl TimeSource for WasmClock {
        fn now_ms(&self) -> u64 {
            window()
                .and_then(|w| w.performance())
                .map(|p| p.now() as u64)
                .unwrap_or(0)
        }
    }
    Executor::install_time_source(&ex, std::rc::Rc::new(WasmClock));

    // On Wasm, main() returns after setup.  If the scope is dropped
    // at that point, `cancelled` is set to true and subsequent spawns
    // silently return.  Move the only reference into the button
    // closure (which is forget'd via the event listener) so it lives
    // as long as the page.
    let scope_auto = TaskScope::with_executor(&ex);

    let auto_running = Rc::new(Cell::new(false));

    // ---- display loop (setInterval ~60fps) ----
    let cnt_display = count.clone();
    let ex_display = ex.clone();
    let closure = Closure::wrap(Box::new(move || {
        Executor::flush_instance(&ex_display);
        let v = cnt_display.read();
        set_text("count", &v.to_string());
        set_text("doubled", &format!("(doubled: {})", v * 2));
    }) as Box<dyn FnMut()>);

    window()
        .unwrap()
        .set_interval_with_callback_and_timeout_and_arguments(
            closure.as_ref().unchecked_ref(),
            16,
            &js_sys::Array::new(),
        )
        .unwrap();
    closure.forget();

    // ---- buttons ----
    let c = count.clone();
    button("inc", move || c.set(c.read() + 1));

    let c = count.clone();
    button("dec", move || c.set(c.read() - 1));

    let c = count.clone();
    let running = Rc::clone(&auto_running);
    let ex_auto = ex.clone();
    button("auto", move || {
        if !running.get() {
            running.set(true);
            let c2 = c.clone();
            let r2 = Rc::clone(&running);
            scope_auto.spawn(async move {
                while r2.get() {
                    timer::sleep(Duration::from_secs(1)).await;
                    c2.set(c2.read() + 1);
                }
            });
            // Wasm has no ScheduleFlush — manually flush so the
            // newly-spawned task is polled without waiting for the
            // next setInterval tick.
            Executor::flush_instance(&ex_auto);
        }
    });

    button("stop", move || auto_running.set(false));

    set_text("count", "0");
    set_text("doubled", "(doubled: 0)");
}
