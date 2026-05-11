/// Signal performance benchmarks.
///
/// Run with: `cargo run --example signal_bench --release`
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

use auralis_signal::Signal;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

struct NoopWaker;
impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}
fn noop_waker() -> Waker {
    Waker::from(Arc::new(NoopWaker))
}

fn poll<T>(fut: Pin<&mut impl std::future::Future<Output = T>>, waker: &Waker) -> Poll<T> {
    let mut cx = Context::from_waker(waker);
    fut.poll(&mut cx)
}

// ---------------------------------------------------------------------------
// Timer helper
// ---------------------------------------------------------------------------

fn time(label: &str, iterations: u64, mut f: impl FnMut()) {
    // Warm-up
    for _ in 0..(iterations / 10).max(1) {
        f();
    }
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let elapsed = start.elapsed().as_secs_f64() * 1_000_000_000.0 / iterations as f64;
    if elapsed < 1_000.0 {
        println!("  {:<42} {:>8.2} ns/iter", label, elapsed);
    } else {
        println!("  {:<42} {:>8.2} µs/iter", label, elapsed / 1_000.0);
    }
}

// ---------------------------------------------------------------------------

fn main() {
    println!("=== Auralis Signal Benchmarks ===\n");

    // 1. set throughput
    time("signal_set_throughput", 10_000_000, || {
        let sig = Signal::new(0u64);
        for i in 0..100 {
            sig.set(i);
        }
    });

    // 1b. set per-op
    {
        let sig = Signal::new(0u64);
        let n = 1_000_000u64;
        let start = Instant::now();
        for i in 0..n {
            sig.set(i);
        }
        let elapsed = start.elapsed().as_secs_f64() * 1_000_000_000.0 / n as f64;
        println!("  {:<42} {:>8.2} ns/set", "signal_set_single", elapsed);
    }

    // 2. changed wake latency
    time("signal_changed_wake_latency", 100_000, || {
        let sig = Signal::new(0i32);
        let waker = noop_waker();
        let mut fut = Box::pin(sig.changed());
        assert!(poll(fut.as_mut(), &waker).is_pending());
        sig.set(1);
        assert!(poll(fut.as_mut(), &waker).is_ready());
        sig.set(0);
    });

    // 3. mass-wake: N waiters
    for &n in &[10u32, 100, 1000] {
        let label = format!("signal_mass_wake_{n}_waiters");
        let iterations = if n <= 10 {
            500_000
        } else if n <= 100 {
            50_000
        } else {
            5_000
        };
        time(&label, iterations, || {
            let sig = Signal::new(0i32);
            let waker = noop_waker();
            let mut futs: Vec<_> = (0..n)
                .map(|_| {
                    let mut f = Box::pin(sig.changed());
                    let _ = poll(f.as_mut(), &waker);
                    f
                })
                .collect();
            sig.set(42);
            for fut in &mut futs {
                assert!(poll(fut.as_mut(), &waker).is_ready());
            }
            sig.set(0);
        });
    }

    // 4. no waker accumulation
    time("signal_no_waker_accumulation", 10_000, || {
        let sig = Signal::new(0i32);
        let waker = noop_waker();
        for _ in 0..100 {
            let mut fut = Box::pin(sig.changed());
            let _ = poll(fut.as_mut(), &waker);
        }
    });

    println!("\n=== Done ===");
}
