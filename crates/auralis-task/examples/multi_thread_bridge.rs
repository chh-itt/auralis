//! Multi-thread bridge: feed a [`Signal<T>`] from another thread using
//! only `std::sync::mpsc`.  No extra crate needed.
//!
//! `Signal<T>` is `!Send + !Sync` by design — it lives on the executor
//! thread, so you can't move it into [`std::thread::spawn`].  The
//! solution is a standard mpsc channel: the worker thread owns the
//! **sender** (which *is* `Send`), and the host thread drains the
//! **receiver** and calls `sig.set()`.
//!
//! ```bash
//! cargo run --example multi_thread_bridge -p auralis-task
//! ```

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use auralis_signal::Signal;

// ---------------------------------------------------------------------------
// Reusable helpers (copy into your project)
// ---------------------------------------------------------------------------

/// Drain every message from the receiver and `set` it into the signal.
///
/// Blocking — runs on the signal's owning thread until all senders are
/// dropped and the channel is empty.
fn drain_into<T: Clone + 'static>(sig: &Signal<T>, rx: mpsc::Receiver<T>) {
    for msg in rx {
        sig.set(msg);
    }
}

// ---------------------------------------------------------------------------
// Demo
// ---------------------------------------------------------------------------

fn main() {
    // -- Pattern 1: inline sync bridge (the classic 6-line recipe) -----------
    println!("=== 1. inline sync bridge ===");
    {
        let counter = Signal::new(0i32);
        let (tx, rx) = mpsc::channel::<i32>();

        // Worker: only the Sender crosses threads (Sender is Send).
        let handle = thread::spawn(move || {
            for i in 1..=5 {
                tx.send(i).unwrap();
            }
            // tx dropped → channel closes → rx loop on main thread exits.
        });

        // Host thread: receive and set.
        for msg in rx {
            counter.set(msg);
            println!("   signal <- {msg}");
        }
        handle.join().unwrap();
        assert_eq!(counter.read(), 5);
    }

    // -- Pattern 2: multiple producers → one signal -------------------------
    println!("\n=== 2. multiple producers ===");
    {
        let counter = Signal::new(0i32);
        let (tx, rx) = mpsc::channel::<i32>();

        let tx1 = tx.clone();
        let tx2 = tx.clone();
        let t1 = thread::spawn(move || {
            for i in 1..=3 {
                tx1.send(i).unwrap();
                thread::sleep(Duration::from_millis(2));
            }
        });
        let t2 = thread::spawn(move || {
            for i in 4..=6 {
                tx2.send(i).unwrap();
                thread::sleep(Duration::from_millis(2));
            }
        });
        drop(tx); // original sender gone → channel closes when workers finish

        drain_into(&counter, rx);
        t1.join().unwrap();
        t2.join().unwrap();
        println!("   final value: {}", counter.read());
    }

    // -- Pattern 3: oneshot result from a compute thread --------------------
    println!("\n=== 3. oneshot result ===");
    {
        let result = Signal::new(String::new());
        let (tx, rx) = mpsc::channel::<String>();

        thread::spawn(move || {
            let computed = format!("computed on {:?}", thread::current().id());
            tx.send(computed).unwrap();
        })
        .join()
        .unwrap();

        drain_into(&result, rx);
        println!("   result: {}", result.read());
    }

    println!("\nAll patterns completed — no extra crate needed.");
}
