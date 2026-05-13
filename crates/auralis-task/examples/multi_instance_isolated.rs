//! Multi-instance executor isolation: each "request" gets its own
//! `Executor + TaskScope + Signal`, with zero cross-request leakage.
//!
//! ```bash
//! cargo run --example multi_instance_isolated -p auralis-task
//! ```

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use auralis_signal::Signal;
use auralis_task::{Executor, ScheduleFlush, TaskScope};

// -- synchronous flush scheduler --------------------------------------------
struct SyncScheduler;
impl ScheduleFlush for SyncScheduler {
    fn schedule(&self, callback: Box<dyn FnOnce()>) {
        callback();
    }
}

// -- simulated "request handler" --------------------------------------------
//
// Each request creates its own Executor + Scope + Signals.  The key
// guarantee: nothing leaks between requests — drop the scope and
// everything is cleaned up.

fn handle_request(request_id: u32) -> String {
    let ex = Executor::new_instance();
    Executor::install_flush_scheduler(&ex, Rc::new(SyncScheduler));

    let scope = TaskScope::with_executor(&ex);
    let data = Signal::new(String::new());

    // "route handler" spawns a reactive effect
    let d = data.clone();
    scope.spawn(async move {
        loop {
            d.changed().await;
            let val = d.read();
            if val == "done" {
                break;
            }
        }
    });

    // "middleware" sets up cleanup
    let cleanup_called = Rc::new(Cell::new(false));
    let cc = Rc::clone(&cleanup_called);
    scope.on_cleanup(move || cc.set(true));

    // "business logic" — would be I/O in real app
    let result = auralis_task::with_executor(&ex, || {
        data.set(format!("request {request_id}: processing"));
        data.set(format!("request {request_id}: done"));
        data.read()
    });

    // Drop scope → cancels effect, runs cleanup.
    drop(scope);

    assert!(cleanup_called.get());
    result
}

fn main() {
    // ---- Pattern 1: sequential requests (same thread, isolated) ----------
    println!("=== 1. Sequential request isolation ===");
    let r1 = handle_request(1);
    let r2 = handle_request(2);
    println!("  {r1}");
    println!("  {r2}");
    assert_eq!(r1, "request 1: done");
    assert_eq!(r2, "request 2: done");

    // ---- Pattern 2: same-signal isolation test (different executors) -----
    println!("\n=== 2. Cross-executor signal isolation ===");
    {
        let ex_a = Executor::new_instance();
        Executor::install_flush_scheduler(&ex_a, Rc::new(SyncScheduler));
        let ex_b = Executor::new_instance();
        Executor::install_flush_scheduler(&ex_b, Rc::new(SyncScheduler));

        let sig_a1 = Signal::new(0i32);
        let sig_b1 = Signal::new(0i32);

        // Scope on executor A sets sig_a and reads sig_b.
        let scope_a = TaskScope::with_executor(&ex_a);
        let a_val = Rc::new(RefCell::new(Vec::new()));
        let av = Rc::clone(&a_val);
        {
            let s = sig_a1.clone();
            scope_a.spawn(async move {
                s.set(42);
                av.borrow_mut().push(s.read());
            });
        }
        drop(scope_a);

        // Scope on executor B sets sig_b and reads sig_a.
        let scope_b = TaskScope::with_executor(&ex_b);
        let b_val = Rc::new(RefCell::new(Vec::new()));
        let bv = Rc::clone(&b_val);
        {
            let s = sig_b1.clone();
            scope_b.spawn(async move {
                s.set(99);
                bv.borrow_mut().push(s.read());
            });
        }
        drop(scope_b);

        // Each executor only sees its own signal changes.
        assert_eq!(*a_val.borrow(), vec![42]);
        assert_eq!(*b_val.borrow(), vec![99]);
        assert_eq!(sig_a1.read(), 42);
        assert_eq!(sig_b1.read(), 99);
    }

    // ---- Pattern 3: the SSR mental model (one request = one executor) ----
    println!("\n=== 3. SSR mental model ===");
    for i in 0..3 {
        let ex = Executor::new_instance();
        Executor::install_flush_scheduler(&ex, Rc::new(SyncScheduler));
        let counter = Signal::new(0i32);

        let scope = TaskScope::with_executor(&ex);
        let c = counter.clone();
        let results = Rc::new(RefCell::new(Vec::new()));
        let res = Rc::clone(&results);

        scope.spawn(async move {
            loop {
                c.changed().await;
                let v = c.read();
                res.borrow_mut().push(v);
                if v >= 3 {
                    break;
                }
            }
        });

        auralis_task::with_executor(&ex, || {
            counter.set(1);
            counter.set(2);
            counter.set(3);
        });

        drop(scope);
        assert_eq!(*results.borrow(), vec![1, 2, 3]);
        println!("  request {i}: signals {:?}", results.borrow());
    }

    println!("\nAll patterns completed — zero cross-request leakage.");
}
