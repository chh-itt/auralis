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

fn log(s: &str) {
    web_sys::console::log_1(&JsValue::from_str(s));
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
    // ---- reactive state ----
    let count = Signal::new(0i32);

    // ---- executor + TimeSource (performance.now) ----
    let ex = Executor::new_instance();

    // Without a TimeSource, timer::sleep degrades to single-flush yield.
    // Use performance.now() for real millisecond-precision timing in Wasm.
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

    let scope = TaskScope::with_executor(&ex);
    let auto_running = Rc::new(Cell::new(false));

    // ---- sync display to signal via timer ----
    // In a real app you'd use requestAnimationFrame; here we use
    // setInterval to flush the executor and update the DOM.
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
            16, // ~60fps
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
    let ex_for_auto = ex.clone();
    button("auto", move || {
        log(&format!(
            "[auto] clicked — running={} active_tasks={}",
            running.get(),
            ex_for_auto.borrow().active_task_count(),
        ));
        if !running.get() {
            running.set(true);
            let c2 = c.clone();
            let r2 = Rc::clone(&running);
            let ex_spawn = ex_for_auto.clone();
            // Spawn directly on the executor instead of through TaskScope,
            // to eliminate any scope registration / cancellation issues on Wasm.
            Executor::spawn(&ex_spawn, async move {
                log("[auto] task started");
                while r2.get() {
                    timer::sleep(Duration::from_secs(1)).await;
                    c2.set(c2.read() + 1);
                }
                log("[auto] task exited (running=false)");
            });
            // Flush immediately so the task gets polled.
            Executor::flush_instance(&ex_for_auto);
            log("[auto] spawn+flush done");
        }
    });

    button("stop", move || {
        log("[stop] clicked");
        auto_running.set(false);
    });

    // ---- init ----
    set_text("count", "0");
    set_text("doubled", "(doubled: 0)");
}
