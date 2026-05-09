/// Criterion benchmarks for auralis-signal.
///
/// Run with: `cargo bench -p auralis-signal`
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

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
// benchmark 1: Signal::set throughput
// ---------------------------------------------------------------------------

fn bench_set_throughput(c: &mut Criterion) {
    c.bench_function("signal_set_100", |b| {
        let sig = Signal::new(0i32);
        let mut val = 0;
        b.iter(|| {
            sig.set(val);
            val += 1;
        });
    });
}

// ---------------------------------------------------------------------------
// benchmark 2: Signal::changed wake latency
// ---------------------------------------------------------------------------

fn bench_changed_latency(c: &mut Criterion) {
    c.bench_function("signal_changed_wake", |b| {
        let sig = Signal::new(0i32);
        let waker = noop_waker();

        b.iter(|| {
            let mut fut = Box::pin(sig.changed());
            // First poll registers the waker (pending).
            assert!(poll(fut.as_mut(), &waker).is_pending());
            // Trigger the change.
            sig.set(1);
            // Second poll should be ready.
            assert!(poll(fut.as_mut(), &waker).is_ready());
            // Reset for next iteration.
            sig.set(0);
        });
    });
}

// ---------------------------------------------------------------------------
// benchmark 3: mass-wake — one signal, N waiters
// ---------------------------------------------------------------------------

fn bench_mass_wake(c: &mut Criterion) {
    for &n in &[10, 100, 1000] {
        let name = format!("signal_mass_wake_{}_waiters", n);
        c.bench_function(&name, |b| {
            let sig = Signal::new(0i32);
            let waker = noop_waker();

            // Create N futures, all polled once to register wakers.
            let mut futs: Vec<_> = (0..n)
                .map(|_| {
                    let mut f = Box::pin(sig.changed());
                    let _ = poll(f.as_mut(), &waker);
                    f
                })
                .collect();

            // The actual bench: one set(), then re-poll each future.
            b.iter(|| {
                sig.set(black_box(42));
                for fut in &mut futs {
                    assert!(poll(fut.as_mut(), &waker).is_ready());
                }
                sig.set(0);
            });
        });
    }
}

// ---------------------------------------------------------------------------
// benchmark 4: long-lived signal — no waker accumulation
// ---------------------------------------------------------------------------

fn bench_no_waker_leak(c: &mut Criterion) {
    c.bench_function("signal_no_waker_accumulation", |b| {
        let sig = Signal::new(0i32);
        let waker = noop_waker();

        b.iter(|| {
            for _ in 0..100 {
                let mut fut = Box::pin(sig.changed());
                let _ = poll(fut.as_mut(), &waker);
                // fut is dropped here — must deregister its waker
            }
            // Waker count should be back to 0 after all drops.
            // (debug_count_waiters is #[cfg(test)] only; verified by
            //  unit tests — this bench just measures throughput.)
            black_box(());
        });
    });
}

criterion_group!(
    benches,
    bench_set_throughput,
    bench_changed_latency,
    bench_mass_wake,
    bench_no_waker_leak,
);
criterion_main!(benches);
