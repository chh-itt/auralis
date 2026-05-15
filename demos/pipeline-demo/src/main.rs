//! Sensor Data Pipeline Demo
//!
//! A realistic reactive data pipeline exercising all devtools features:
//!
//! - 4 raw sensor signals (temperature, humidity, pressure, wind)
//! - 8 memos forming a dependency chain: validate → normalize → aggregate → alert → format
//! - 3 task scopes (Pipeline → SensorSimulator, Monitoring)
//! - Periodic batch updates, timer-driven tasks, labeled signals/memos/scopes
//!
//! Usage:
//! ```bash
//! cargo run -p pipeline-demo           # Run the pipeline, dump snapshot on exit
//! cargo run -p pipeline-demo -- devtools # Print snapshot every 3 seconds
//! ```

use std::io::Write;
use std::rc::Rc;
use std::time::Duration;

use auralis_signal::{batch, Memo, Signal};
use auralis_task::init_flush_scheduler;
use auralis_task::timer;
use auralis_task::TaskScope;

// ---------------------------------------------------------------------------
// Scheduler: synchronous flush (like Wasm / game loop)
// ---------------------------------------------------------------------------

struct SyncScheduler;

impl auralis_task::ScheduleFlush for SyncScheduler {
    fn schedule(&self, callback: Box<dyn FnOnce()>) {
        callback();
    }
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

#[allow(dead_code)]
struct Pipeline {
    // Raw sensor signals
    temperature: Signal<f64>,
    humidity: Signal<f64>,
    pressure: Signal<f64>,
    wind_speed: Signal<f64>,

    // Validation memos (range checks)
    temp_valid: Memo<Option<f64>>,
    humidity_valid: Memo<Option<f64>>,
    pressure_valid: Memo<Option<f64>>,
    wind_valid: Memo<Option<f64>>,

    // Normalized values (0.0 - 1.0)
    temp_norm: Memo<f64>,
    humidity_norm: Memo<f64>,
    pressure_norm: Memo<f64>,
    wind_norm: Memo<f64>,

    // Aggregate memos
    avg_normalized: Memo<f64>,
    active_sensors: Memo<usize>,

    // Alert memos
    any_alert: Memo<bool>,
    alert_message: Memo<String>,

    // Format memo
    dashboard: Memo<String>,
}

impl Pipeline {
    fn new() -> Self {
        // ---- Raw sensors ----
        let temperature = Signal::new(25.0);
        temperature.set_label("temperature");
        let humidity = Signal::new(60.0);
        humidity.set_label("humidity");
        let pressure = Signal::new(1013.0);
        pressure.set_label("pressure");
        let wind_speed = Signal::new(5.0);
        wind_speed.set_label("wind_speed");

        // ---- Validation (range checks return Some or None) ----
        let t = temperature.clone();
        let temp_valid = Memo::new(move || {
            let v = t.read();
            if (-50.0..=60.0).contains(&v) { Some(v) } else { None }
        });
        temp_valid.set_label("temp_valid");

        let h = humidity.clone();
        let humidity_valid = Memo::new(move || {
            let v = h.read();
            if (0.0..=100.0).contains(&v) { Some(v) } else { None }
        });
        humidity_valid.set_label("humidity_valid");

        let p = pressure.clone();
        let pressure_valid = Memo::new(move || {
            let v = p.read();
            if (800.0..=1200.0).contains(&v) { Some(v) } else { None }
        });
        pressure_valid.set_label("pressure_valid");

        let w = wind_speed.clone();
        let wind_valid = Memo::new(move || {
            let v = w.read();
            if v >= 0.0 { Some(v) } else { None }
        });
        wind_valid.set_label("wind_valid");

        // ---- Normalization (0.0 - 1.0, default 0.0 if invalid) ----
        let tv = temp_valid.clone();
        let temp_norm = Memo::new(move || {
            tv.with(|v| v.map_or(0.0, |val| (val + 50.0) / 110.0))
        });
        temp_norm.set_label("temp_norm");

        let hv = humidity_valid.clone();
        let humidity_norm = Memo::new(move || {
            hv.with(|v| v.map_or(0.0, |val| val / 100.0))
        });
        humidity_norm.set_label("humidity_norm");

        let pv = pressure_valid.clone();
        let pressure_norm = Memo::new(move || {
            pv.with(|v| v.map_or(0.0, |val| (val - 800.0) / 400.0))
        });
        pressure_norm.set_label("pressure_norm");

        let wv = wind_valid.clone();
        let wind_norm = Memo::new(move || {
            wv.with(|v| v.map_or(0.0_f64, |val| (val / 50.0_f64).min(1.0)))
        });
        wind_norm.set_label("wind_norm");

        // ---- Aggregates ----
        let tn = temp_norm.clone();
        let hn = humidity_norm.clone();
        let pn = pressure_norm.clone();
        let wn = wind_norm.clone();
        let avg_normalized = Memo::new(move || {
            (tn.read() + hn.read() + pn.read() + wn.read()) / 4.0
        });
        avg_normalized.set_label("avg_normalized");

        let tv2 = temp_valid.clone();
        let hv2 = humidity_valid.clone();
        let pv2 = pressure_valid.clone();
        let wv2 = wind_valid.clone();
        let active_sensors = Memo::new(move || {
            [&tv2, &hv2, &pv2, &wv2]
                .iter()
                .filter(|m| m.with(|v| v.is_some()))
                .count()
        });
        active_sensors.set_label("active_sensors");

        // ---- Alerts ----
        let an = avg_normalized.clone();
        let an2 = avg_normalized.clone();
        let ac = active_sensors.clone();
        let any_alert = Memo::new(move || {
            an.read() > 0.85 || ac.read() < 4
        });
        any_alert.set_label("any_alert");

        let t2 = temperature.clone();
        let h2 = humidity.clone();
        let p2 = pressure.clone();
        let w2 = wind_speed.clone();
        let ac2 = active_sensors.clone();
        let aa2 = any_alert.clone();
        let alert_message = Memo::new(move || {
            if aa2.read() {
                format!(
                    "ALERT: avg_norm={:.3}, active={}, T={:.1} H={:.1} P={:.1} W={:.1}",
                    an2.read(),
                    ac2.read(),
                    t2.read(),
                    h2.read(),
                    p2.read(),
                    w2.read()
                )
            } else {
                String::from("OK")
            }
        });
        alert_message.set_label("alert_message");

        // ---- Dashboard (formatted output) ----
        let t3 = temperature.clone();
        let h3 = humidity.clone();
        let p3 = pressure.clone();
        let w3 = wind_speed.clone();
        let ac3 = active_sensors.clone();
        let an2 = avg_normalized.clone();
        let am2 = alert_message.clone();
        let dashboard = Memo::new(move || {
            format!(
                "┌─ Sensor Dashboard ─────────────────────┐\n\
                 │ T={:>6.1}°C  H={:>6.1}%  P={:>7.1}hPa  W={:>5.1}m/s │\n\
                 │ active: {}  avg_norm: {:.3}            │\n\
                 │ {} │\n\
                 └────────────────────────────────────────┘",
                t3.read(),
                h3.read(),
                p3.read(),
                w3.read(),
                ac3.read(),
                an2.read(),
                am2.read(),
            )
        });
        dashboard.set_label("dashboard");

        Self {
            temperature,
            humidity,
            pressure,
            wind_speed,
            temp_valid,
            humidity_valid,
            pressure_valid,
            wind_valid,
            temp_norm,
            humidity_norm,
            pressure_norm,
            wind_norm,
            avg_normalized,
            active_sensors,
            any_alert,
            alert_message,
            dashboard,
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    init_flush_scheduler(Rc::new(SyncScheduler));

    let pipeline = Rc::new(Pipeline::new());
    let root = TaskScope::new();
    root.set_label("Pipeline");

    // ---- Child scope: Sensor Simulator ----
    let sim_scope = TaskScope::new_child(&root);
    sim_scope.set_label("SensorSimulator");

    let t = pipeline.temperature.clone();
    let h = pipeline.humidity.clone();
    let p = pipeline.pressure.clone();
    let w = pipeline.wind_speed.clone();
    sim_scope.spawn(async move {
        loop {
            timer::sleep(Duration::from_millis(800)).await;
            batch(|| {
                t.update(|v| *v += rand_offset(0.5));
                h.update(|v| *v += rand_offset(1.0));
                p.update(|v| *v += rand_offset(2.0));
                w.update(|v| *v = (*v + rand_offset(0.3)).max(0.0));
            });
        }
    });

    // ---- Child scope: Monitoring ----
    let mon_scope = TaskScope::new_child(&root);
    mon_scope.set_label("Monitoring");

    // Alert watcher — prints when alerts fire
    let alert = pipeline.alert_message.clone();
    mon_scope.spawn(async move {
        loop {
            alert.changed().await;
            let msg = alert.read();
            if msg != "OK" {
                println!("  \x1b[31m{}\x1b[0m", msg);
            }
        }
    });

    // Report generator — prints dashboard every 3 seconds
    let dashboard = pipeline.dashboard.clone();
    let report_scope = TaskScope::new_child(&mon_scope);
    report_scope.set_label("ReportGenerator");
    let dash = dashboard.clone();
    report_scope.spawn(async move {
        loop {
            timer::sleep(Duration::from_secs(3)).await;
            println!();
            println!("{}", dash.read());
        }
    });

    // ---- DevTools integration ----
    let args: Vec<String> = std::env::args().collect();
    let devtools_mode = args.get(1).map(String::as_str) == Some("devtools");

    let snap = auralis_devtools::snapshot();
    println!("=== Pipeline Started ===");
    println!(
        "{} signals, {} memos, {} scopes",
        snap.signals.len(),
        snap.memos.len(),
        3 // Pipeline, SensorSimulator, Monitoring
    );
    println!();

    if devtools_mode {
        // Run with periodic devtools snapshots
        let dev_scope = TaskScope::new_child(&root);
        dev_scope.set_label("Devtools");
        dev_scope.spawn(async move {
            loop {
                timer::sleep(Duration::from_secs(3)).await;
                let snap = auralis_devtools::snapshot();
                let json = serde_json::to_string_pretty(&snap).unwrap();
                println!("=== DevTools Snapshot ===");
                println!("{json}");
                let _ = std::io::stdout().flush();
            }
        });

        // Run for ~18 seconds
        run_loop(18);
    } else {
        // Default mode: run for ~12 seconds, dump snapshot at end
        run_loop(12);

        println!();
        println!("=== Final Reactive Graph ===");
        println!("{}", auralis_task::dump_reactive_graph());

        println!();
        println!("=== Memo Dependency Graph ===");
        print_dep_graph(&pipeline);

        let final_snap = auralis_devtools::snapshot();
        let json = serde_json::to_string_pretty(&final_snap).unwrap();
        println!();
        println!("=== Final JSON Snapshot ===");
        println!("{json}");
    }

    // Clean shutdown — dropping root cancels all tasks.
    drop(root);
    println!("\nPipeline shut down.");
}

fn run_loop(seconds: u64) {
    let end = std::time::Instant::now() + Duration::from_secs(seconds);
    // Busy-loop: the SyncScheduler runs callbacks synchronously, so
    // timer::sleep triggers immediately on the next flush.
    while std::time::Instant::now() < end {
        // Each iteration, any pending timers fire synchronously
        // through the flush scheduler.
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn rand_offset(range: f64) -> f64 {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as f64;
    let r = (seed.sin() * 1000.0).fract();
    (r - 0.5) * 2.0 * range
}

fn print_dep_graph(p: &Pipeline) {
    show_deps(&p.dashboard, "dashboard");
    show_deps(&p.alert_message, "alert_message");
    show_deps(&p.any_alert, "any_alert");
    show_deps(&p.active_sensors, "active_sensors");
    show_deps(&p.avg_normalized, "avg_normalized");
}

fn show_deps<T: Clone + 'static>(m: &Memo<T>, label: &str) {
    let addrs = m.dependency_addrs();
    println!(
        "  \"{label}\" ({}) → {} deps",
        m.label().unwrap_or_default(),
        addrs.len()
    );
    for a in &addrs {
        println!("    depends on addr {a:#x}");
    }
}
