//! Standalone: initialise the pipeline, run for a few seconds, dump
//! the devtools JSON snapshot to `pipeline_snapshot.json`.
//!
//! ```bash
//! cargo run --example dump_snapshot -p pipeline-demo
//! ```

use std::io::Write;
use std::rc::Rc;
use std::time::Duration;

use auralis_signal::batch;
use auralis_task::{init_flush_scheduler, timer, TaskScope};

// Re-use the Pipeline definition from the main binary.
// In a real project this would be a shared library; for a demo,
// we duplicate the struct minimally.
mod pipeline {
    use auralis_signal::{Memo, Signal};
    #[allow(dead_code)]
    pub struct Pipeline {
        pub temperature: Signal<f64>,
        pub humidity: Signal<f64>,
        pub pressure: Signal<f64>,
        pub wind_speed: Signal<f64>,
        pub avg_normalized: Memo<f64>,
        pub dashboard: Memo<String>,
    }

    pub fn build() -> Pipeline {
        let temperature = Signal::new(25.0);
        temperature.set_label("temperature");
        let humidity = Signal::new(60.0);
        humidity.set_label("humidity");
        let pressure = Signal::new(1013.0);
        pressure.set_label("pressure");
        let wind_speed = Signal::new(5.0);
        wind_speed.set_label("wind_speed");

        let t = temperature.clone();
        let h = humidity.clone();
        let p = pressure.clone();
        let w = wind_speed.clone();
        let avg_normalized = Memo::new(move || {
            let tv = (t.read() + 50.0) / 110.0;
            let hv = h.read() / 100.0;
            let pv = (p.read() - 800.0) / 400.0;
            let wv = (w.read() / 50.0_f64).min(1.0);
            (tv + hv + pv + wv) / 4.0
        });
        avg_normalized.set_label("avg_normalized");

        let avg = avg_normalized.clone();
        let t2 = temperature.clone();
        let h2 = humidity.clone();
        let p2 = pressure.clone();
        let w2 = wind_speed.clone();
        let dashboard = Memo::new(move || {
            format!(
                "T={:.1} H={:.1} P={:.1} W={:.1} avg={:.3}",
                t2.read(),
                h2.read(),
                p2.read(),
                w2.read(),
                avg.read()
            )
        });
        dashboard.set_label("dashboard");

        Pipeline {
            temperature,
            humidity,
            pressure,
            wind_speed,
            avg_normalized,
            dashboard,
        }
    }
}

struct SyncScheduler;
impl auralis_task::ScheduleFlush for SyncScheduler {
    fn schedule(&self, callback: Box<dyn FnOnce()>) {
        callback();
    }
}

fn main() {
    init_flush_scheduler(Rc::new(SyncScheduler));

    let p = pipeline::build();
    let scope = TaskScope::new();
    scope.set_label("Pipeline");

    // Simulate sensor updates.
    let t = p.temperature.clone();
    let h = p.humidity.clone();
    let p_sig = p.pressure.clone();
    let w = p.wind_speed.clone();
    scope.spawn(async move {
        for _ in 0..8 {
            timer::sleep(Duration::from_millis(500)).await;
            batch(|| {
                t.update(|v| *v += (fast_rand() - 0.5) * 2.0);
                h.update(|v| *v += (fast_rand() - 0.5) * 4.0);
                p_sig.update(|v| *v += (fast_rand() - 0.5) * 10.0);
                w.update(|v| *v = (*v + (fast_rand() - 0.5) * 1.0).max(0.0));
            });
        }
    });

    // Run for ~5 seconds.
    let end = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < end {
        std::thread::sleep(Duration::from_millis(100));
    }

    drop(scope);

    // Dump snapshot.
    let snap = auralis_devtools::snapshot();
    let json = serde_json::to_string_pretty(&snap).unwrap();
    let path = "pipeline_snapshot.json";
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(json.as_bytes()).unwrap();
    println!("Snapshot written to {path}");
    println!("  signals: {}", snap.signals.len());
    println!("  memos:   {}", snap.memos.len());
    for m in &snap.memos {
        println!(
            "  memo \"{}\" deps={} dirty={} computed={}x",
            m.label.as_deref().unwrap_or("?"),
            m.dependency_addrs.len(),
            m.is_dirty,
            m.compute_count
        );
    }
}

fn fast_rand() -> f64 {
    use std::time::SystemTime;
    let n = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as f64;
    (n.sin() * 1000.0).fract()
}
