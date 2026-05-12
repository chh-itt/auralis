use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use auralis_signal::{batch, Memo, Signal};
use auralis_task::{timer, Executor, JoinHandle, TaskScope, TimeSource};
use wasm_bindgen::prelude::*;
use web_sys::window;

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

fn set_text(id: &str, text: &str) {
    if let Some(el) = window()
        .and_then(|w| w.document())
        .and_then(|d| d.get_element_by_id(id))
    {
        el.set_text_content(Some(text));
    }
}

fn set_html(id: &str, html: &str) {
    if let Some(el) = window()
        .and_then(|w| w.document())
        .and_then(|d| d.get_element_by_id(id))
    {
        el.set_inner_html(html);
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

// ---------------------------------------------------------------------------
// Task entry — cooperative cancellation via `running` flag
// ---------------------------------------------------------------------------

struct TaskEntry {
    id: u32,
    signal: Signal<i32>,
    running: Rc<Cell<bool>>,
    _handle: JoinHandle,
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[wasm_bindgen(start)]
pub fn main() {
    // -- Common: executor + time source ----------------------------------

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
    Executor::install_time_source(&ex, Rc::new(WasmClock));

    // -- Section 1: Counter (Signal) ------------------------------------

    let count = Signal::new(0i32);

    let c = count.clone();
    button("inc", move || c.update(|v| *v += 1));
    let c = count.clone();
    button("dec", move || c.update(|v| *v -= 1));
    let c = count.clone();
    button("reset-counter", move || c.set(0));

    // -- Section 2: Memo (auto-tracking derived values) ------------------

    let a = Signal::new(0i32);
    let b = Signal::new(0i32);
    let sum = Memo::new({
        let a = a.clone();
        let b = b.clone();
        move || a.read() + b.read()
    });
    let product = Memo::new({
        let a = a.clone();
        let b = b.clone();
        move || a.read() * b.read()
    });

    button("a-inc", {
        let a = a.clone();
        move || a.update(|v| *v += 1)
    });
    button("a-dec", {
        let a = a.clone();
        move || a.update(|v| *v -= 1)
    });
    button("b-inc", {
        let b = b.clone();
        move || b.update(|v| *v += 1)
    });
    button("b-dec", {
        let b = b.clone();
        move || b.update(|v| *v -= 1)
    });

    // -- Section 3: Task Lifecycle (structured concurrency) --------------

    let scope: Rc<std::cell::RefCell<TaskScope>> =
        Rc::new(std::cell::RefCell::new(TaskScope::with_executor(&ex)));
    let tasks: Rc<std::cell::RefCell<Vec<TaskEntry>>> =
        Rc::new(std::cell::RefCell::new(Vec::new()));
    let next_task_id: Rc<Cell<u32>> = Rc::new(Cell::new(0));
    let event_log: Rc<std::cell::RefCell<Vec<String>>> =
        Rc::new(std::cell::RefCell::new(Vec::new()));

    // Helper to spawn one task with cooperative cancellation
    let spawn_one = |scope: Rc<std::cell::RefCell<TaskScope>>,
                     tasks: Rc<std::cell::RefCell<Vec<TaskEntry>>>,
                     next_id: Rc<Cell<u32>>,
                     elog: Rc<std::cell::RefCell<Vec<String>>>| {
        let id = next_id.get();
        next_id.set(id + 1);
        let task_signal = Signal::new(0i32);
        let ts = task_signal.clone();
        let running = Rc::new(Cell::new(true));
        let r = running.clone();
        let elog2 = elog.clone();
        let handle = scope.borrow().spawn(async move {
            elog2.borrow_mut().push(format!("task #{} started", id));
            while r.get() {
                timer::sleep(Duration::from_secs(1)).await;
                ts.update(|v| *v += 1);
            }
            elog2.borrow_mut().push(format!("task #{} stopped", id));
        });
        tasks.borrow_mut().push(TaskEntry {
            id,
            signal: task_signal,
            running,
            _handle: handle,
        });
        id
    };

    // Spawn single task
    {
        let scope = scope.clone();
        let tasks = tasks.clone();
        let next_id = next_task_id.clone();
        let elog = event_log.clone();
        button("spawn-task", move || {
            spawn_one(scope.clone(), tasks.clone(), next_id.clone(), elog.clone());
            elog.borrow_mut()
                .push(format!("spawned task ({} active)", tasks.borrow().len()));
        });
    }

    // Spawn 3 tasks
    {
        let scope = scope.clone();
        let tasks = tasks.clone();
        let next_id = next_task_id.clone();
        let elog = event_log.clone();
        button("spawn-3", move || {
            for _ in 0..3 {
                spawn_one(scope.clone(), tasks.clone(), next_id.clone(), elog.clone());
            }
            elog.borrow_mut()
                .push(format!("spawned 3 tasks ({} active)", tasks.borrow().len()));
        });
    }

    // Cancel last task — cooperative: just flip the running flag
    {
        let tasks = tasks.clone();
        let elog = event_log.clone();
        button("cancel-last", move || {
            let popped = tasks.borrow_mut().pop();
            if let Some(entry) = popped {
                entry.running.set(false);
                let remaining = tasks.borrow().len();
                elog.borrow_mut().push(format!(
                    "cancelled task #{} ({} active)",
                    entry.id, remaining
                ));
            }
        });
    }

    // Cancel all — drop scope, create fresh one
    {
        let scope = scope.clone();
        let tasks = tasks.clone();
        let ex_ca = ex.clone();
        let elog = event_log.clone();
        button("cancel-all", move || {
            let count = tasks.borrow().len();
            // Mark all as not running first
            for entry in tasks.borrow().iter() {
                entry.running.set(false);
            }
            // Replace scope (drops old scope, cancels all tasks)
            *scope.borrow_mut() = TaskScope::with_executor(&ex_ca);
            tasks.borrow_mut().clear();
            elog.borrow_mut()
                .push(format!("cancelled all {} tasks (scope replaced)", count));
        });
    }

    // -- Section 4: Batch vs Sequential --------------------------------

    let bx = Signal::new(0i32);
    let by = Signal::new(0i32);
    let bz = Signal::new(0i32);

    // Accumulated notification counts (batch merges multiple set() into fewer
    // internal notifications — we update these manually based on documented
    // library behavior).
    let seq_notify = Rc::new(Cell::new(0u32));
    let batch_notify = Rc::new(Cell::new(0u32));

    // Sequential update — 3 individual set() calls → 3 internal notifications
    {
        let bx = bx.clone();
        let by = by.clone();
        let bz = bz.clone();
        let sn = seq_notify.clone();
        button("seq-update", move || {
            bx.update(|v| *v += 1);
            by.update(|v| *v += 1);
            bz.update(|v| *v += 1);
            sn.set(sn.get() + 3);
        });
    }

    // Batch update — batch() merges 3 set() calls into 1 notification
    {
        let bx = bx.clone();
        let by = by.clone();
        let bz = bz.clone();
        let bn = batch_notify.clone();
        button("batch-update", move || {
            batch(|| {
                bx.update(|v| *v += 1);
                by.update(|v| *v += 1);
                bz.update(|v| *v += 1);
            });
            bn.set(bn.get() + 1);
        });
    }

    // Reset batch demo
    {
        let bx = bx.clone();
        let by = by.clone();
        let bz = bz.clone();
        let sn = seq_notify.clone();
        let bn = batch_notify.clone();
        button("reset-batch", move || {
            batch(|| {
                bx.set(0);
                by.set(0);
                bz.set(0);
            });
            sn.set(0);
            bn.set(0);
        });
    }

    // -- Render loop (runs at ~60 fps, reads everything) ----------------

    let ex_display = ex.clone();
    let closure = Closure::wrap(Box::new(move || {
        Executor::flush_instance(&ex_display);

        // Counter
        set_text("count-val", &count.read().to_string());

        // Memo
        set_text("a-val", &a.read().to_string());
        set_text("b-val", &b.read().to_string());
        set_text("sum-val", &sum.read().to_string());
        set_text("prod-val", &product.read().to_string());

        // Task lifecycle — prune stopped tasks, then render
        let task_html;
        {
            let mut task_list = tasks.borrow_mut();
            task_list.retain(|entry| entry.running.get());
            set_text("active-count", &task_list.len().to_string());
            let mut h = String::new();
            for entry in task_list.iter() {
                h.push_str(&format!(
                    "<span class=\"task-entry\"><span class=\"dot\"></span>#{0}: {1}s</span>",
                    entry.id,
                    entry.signal.read()
                ));
            }
            if task_list.is_empty() {
                h.push_str(
                    "<span style=\"color:var(--dim);font-size:0.85rem\">\
                    No active tasks</span>",
                );
            }
            task_html = h;
        }
        set_html("task-list", &task_html);

        // Batch vs Sequential
        set_text("bx-val", &bx.read().to_string());
        set_text("by-val", &by.read().to_string());
        set_text("bz-val", &bz.read().to_string());
        set_text("seq-count", &seq_notify.get().to_string());
        set_text("batch-count", &batch_notify.get().to_string());

        // Event log (last 25 entries)
        let log = event_log.borrow();
        let start = log.len().saturating_sub(25);
        let mut log_html = String::new();
        for entry in log.iter().skip(start) {
            log_html.push_str(&format!("<div class=\"entry\">{}</div>", entry));
        }
        drop(log);
        set_html("event-log", &log_html);
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
}
