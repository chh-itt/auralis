//! Tokio SSR demo — multi-request isolation with Auralis + Tokio.
//!
//! Each "request" gets its own `Executor + TaskScope + Signals`.
//! Tokio handles I/O (simulated via `time::sleep`); Auralis handles
//! the reactive cascade.  Zero cross-request leakage.
//!
//! **Key pattern:** Tokio does I/O, Auralis does reactivity.  The
//! boundary is `signal.set(value)`.  Use `tokio::join!` for concurrent
//! requests — `!Send` types work fine because the futures stay on the
//! same thread.
//!
//! ```bash
//! cd demos/tokio-ssr && cargo run
//! ```

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use auralis_signal::Signal;
use auralis_task::{Executor, ScheduleFlush, TaskScope};

// -- synchronous flush scheduler -------------------------------------------
struct SyncScheduler;
impl ScheduleFlush for SyncScheduler {
    fn schedule(&self, callback: Box<dyn FnOnce()>) {
        callback();
    }
}

// -- simulated I/O ---------------------------------------------------------
async fn fetch_data(id: u32, ms: u64) -> String {
    tokio::time::sleep(Duration::from_millis(ms)).await;
    format!("data-from-request-{id}")
}

// -- one "request" = one executor + scope + signals ------------------------
//
// The pattern:
//   1. Create per-request Executor + Scope + Signals (no .await)
//   2. Do Tokio I/O and feed results into signals (signal.set)
//   3. Flush Auralis executor to process reactive cascade
//   4. Drop scope → automatic cleanup
//
// Note: `handle_request` is `!Send` (contains `Rc`s).  We use
// `tokio::join!` to poll two requests concurrently on the same thread
// — `tokio::spawn` would require `Send`.
async fn handle_request(request_id: u32) -> (Vec<String>, String) {
    // Per-request isolated executor and scope.
    let ex = Executor::new_instance();
    Executor::install_flush_scheduler(&ex, Rc::new(SyncScheduler));
    let scope = TaskScope::with_executor(&ex);

    let data = Signal::new(String::new());
    let rendered = Rc::new(RefCell::new(Vec::new()));
    let log = Rc::clone(&rendered);

    // Reactive effect: re-render whenever data changes.
    let d = data.clone();
    scope.spawn(async move {
        loop {
            d.changed().await;
            let val = d.read();
            if val.is_empty() {
                continue;
            }
            log.borrow_mut().push(format!("rendered: {val}"));
            if val.contains("done") {
                break;
            }
        }
    });

    // Step 1: Tokio fetches data; Auralis receives it.
    let fetched = fetch_data(request_id, 10).await;
    auralis_task::with_executor(&ex, || data.set(fetched));

    // Step 2: more I/O, then final signal update.
    tokio::time::sleep(Duration::from_millis(5)).await;
    auralis_task::with_executor(&ex, || {
        data.set(format!("request {request_id}: done"));
    });

    // Flush to process all pending reactive updates.
    Executor::flush_instance(&ex);

    let result = data.read();
    let events = rendered.borrow().clone();

    // Drop scope → cancels effect, runs cleanup.  No manual tokens.
    drop(scope);
    (events, result)
}

#[tokio::main]
async fn main() {
    auralis_task::init_scope_store_tokio();

    println!("=== Auralis × Tokio SSR Demo ===\n");

    // Two concurrent requests on the same thread via `tokio::join!`.
    let ((events1, final1), (events2, final2)) = tokio::join!(handle_request(1), handle_request(2));

    println!("Request 1: {final1}");
    for e in &events1 {
        println!("  -> {e}");
    }
    println!();
    println!("Request 2: {final2}");
    for e in &events2 {
        println!("  -> {e}");
    }

    // Verify isolation — each request's state is independent.
    assert_ne!(events1, events2, "each request must have its own state");
    assert!(final1.contains("request 1"));
    assert!(final2.contains("request 2"));

    println!();
    println!("Both requests completed — zero cross-request state leakage.");
    println!("Tokio handled I/O. Auralis handled reactivity. Each request dropped cleanly.");
}
