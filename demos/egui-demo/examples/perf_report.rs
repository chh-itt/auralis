//! Headless performance comparison: plain computation vs Auralis Memo chain.
//!
//! Run: cargo run --example perf_report --release

use std::time::Instant;

use auralis_signal::Signal;

// ---- Copied from data_gen.rs ----
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Clone)]
struct SaleRecord {
    date: u16,
    category: u8,
    region: u8,
    amount: f64,
}

fn generate_sales(count: usize) -> Vec<SaleRecord> {
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_CAFE_4242);
    (0..count)
        .map(|_| SaleRecord {
            date: rng.gen_range(0..365),
            category: rng.gen_range(0..15),
            region: rng.gen_range(0..10),
            amount: rng.gen_range(10.0..10000.0),
        })
        .collect()
}

// ---- Computation pipeline (identical to data_analyzer.rs) ----

#[derive(Clone, PartialEq)]
struct FilterParams {
    date_start: u16,
    date_end: u16,
    categories: [bool; 16],
}

impl Default for FilterParams {
    fn default() -> Self {
        let mut categories = [true; 16];
        categories[15] = false;
        Self {
            date_start: 0,
            date_end: 365,
            categories,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum AggregateMode {
    Median,
    #[allow(dead_code)]
    P95,
}

fn filter_data(records: &[SaleRecord], params: &FilterParams) -> Vec<SaleRecord> {
    records
        .iter()
        .filter(|r| {
            r.date >= params.date_start
                && r.date <= params.date_end
                && params.categories[r.category as usize]
        })
        .cloned()
        .collect()
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn aggregate(records: &[SaleRecord], _mode: AggregateMode) -> (Vec<f64>, Vec<f64>) {
    let n_regions: usize = 10;
    let mut buckets: Vec<Vec<f64>> = vec![Vec::new(); n_regions];
    for r in records {
        buckets[r.region as usize].push(r.amount);
    }
    let mut medians = Vec::with_capacity(n_regions);
    let mut p95s = Vec::with_capacity(n_regions);
    for amounts in buckets.iter_mut() {
        if amounts.is_empty() {
            medians.push(0.0);
            p95s.push(0.0);
        } else {
            amounts.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
            medians.push(percentile(amounts, 0.5));
            p95s.push(percentile(amounts, 0.95));
        }
    }
    (medians, p95s)
}

fn format_results(medians: &[f64], p95s: &[f64]) -> String {
    let mut out = String::new();
    for (i, (m, p)) in medians.iter().zip(p95s.iter()).enumerate() {
        out.push_str(&format!("Region {}: median={:.2} p95={:.2}\n", i, m, p));
    }
    out
}

fn main() {
    println!("=== Auralis Performance Report ===\n");

    let sizes = [100_000, 250_000, 500_000, 1_000_000];

    println!("--- Data Generation ---");
    for &size in &sizes {
        let start = Instant::now();
        let _data = generate_sales(size);
        let elapsed = start.elapsed();
        println!(
            "  {:>7} records: {:>8.2} ms  ({:.1} MB)",
            size,
            elapsed.as_secs_f64() * 1000.0,
            size as f64 * std::mem::size_of::<SaleRecord>() as f64 / (1024.0 * 1024.0)
        );
    }

    println!("\n--- Pipeline: filter → aggregate → format ---");
    for &size in &sizes {
        let data = generate_sales(size);
        let params = FilterParams::default();
        let mode = AggregateMode::Median;

        // Warm up
        let _ = filter_data(&data, &params);

        // Measure: full pipeline (no caching)
        let start = Instant::now();
        let filtered = filter_data(&data, &params);
        let filter_time = start.elapsed();

        let start = Instant::now();
        let (medians, p95s) = aggregate(&filtered, mode);
        let agg_time = start.elapsed();

        let start = Instant::now();
        let _formatted = format_results(&medians, &p95s);
        let fmt_time = start.elapsed();

        let total = filter_time + agg_time + fmt_time;
        println!(
            "  {:>7} records → {:>6} filtered: \
             filter={:>7.2}ms  aggregate={:>7.2}ms  format={:>6.2}ms  TOTAL={:>7.2}ms  \
             (at 60fps budget=16.67ms: {})",
            size,
            filtered.len(),
            filter_time.as_secs_f64() * 1000.0,
            agg_time.as_secs_f64() * 1000.0,
            fmt_time.as_secs_f64() * 1000.0,
            total.as_secs_f64() * 1000.0,
            if total.as_secs_f64() * 1000.0 > 16.67 {
                "DROPS FRAMES"
            } else {
                "OK"
            }
        );
    }

    println!("\n--- Memo Chain (Signal + Memo lazy recompute) ---");
    for &size in &sizes {
        let raw_data = Signal::new(generate_sales(size));
        let filter_params = Signal::new(FilterParams::default());
        let mode_sig = Signal::new(AggregateMode::Median);

        let filtered_memo = auralis_signal::memo!(raw_data, filter_params =>
            filter_data(&raw_data.read(), &filter_params.read())
        );

        let agg_memo = auralis_signal::memo!(filtered_memo, mode_sig =>
            aggregate(&filtered_memo.read(), mode_sig.read())
        );

        let output_memo = auralis_signal::memo!(agg_memo =>
            { let (m, p) = agg_memo.read(); format_results(&m, &p) }
        );

        // First read: all Memos are clean (constructed during new)
        let start = Instant::now();
        let _ = output_memo.read();
        let clean_read = start.elapsed();

        // Dirty the first stage
        filter_params.set(FilterParams {
            date_start: 10,
            ..FilterParams::default()
        });

        // Read again: filter dirty, triggers recompute cascade
        let start = Instant::now();
        let _ = output_memo.read();
        let dirty_read = start.elapsed();

        // Read again: now clean
        let start = Instant::now();
        let _ = output_memo.read();
        let clean_again = start.elapsed();

        println!(
            "  {:>7} records:  clean read={:>7.2}ms  dirty read={:>7.2}ms  clean-again={:>7.2}ms  \
             compute_counts(filter={}, agg={}, fmt={})",
            size,
            clean_read.as_secs_f64() * 1000.0,
            dirty_read.as_secs_f64() * 1000.0,
            clean_again.as_secs_f64() * 1000.0,
            filtered_memo.compute_count(),
            agg_memo.compute_count(),
            output_memo.compute_count(),
        );
    }

    println!("\n--- Signal::set throughput ---");
    let sig = Signal::new(0u64);
    let iterations = 1_000_000;
    let start = Instant::now();
    for i in 0..iterations {
        sig.set(i);
    }
    let elapsed = start.elapsed();
    println!(
        "  {} sets in {:>8.2} ms  ({:.0} sets/ms, {:.0} ns/set)",
        iterations,
        elapsed.as_secs_f64() * 1000.0,
        iterations as f64 / elapsed.as_secs_f64() / 1000.0,
        elapsed.as_secs_f64() * 1_000_000_000.0 / iterations as f64,
    );

    println!("\n--- Three-way comparison: 500K records, 1000 frames ---");

    let data = generate_sales(500_000);
    let params_a = FilterParams::default();
    let params_b = FilterParams {
        date_start: 50,
        date_end: 300,
        ..FilterParams::default()
    };

    let scenarios: &[(&str, usize)] = &[
        ("Params change  1% of frames", 100),
        ("Params change 10% of frames", 10),
        ("Params change 50% of frames", 2),
    ];

    println!(
        "  {:<28} {:>10} {:>14} {:>14}",
        "", "no_cache", "manual_cache", "auralis_memo"
    );
    println!(
        "  {:<28} {:>10} {:>14} {:>14}",
        "", "───────", "───────────", "───────────"
    );

    let mut grand_manual_total = 0.0;
    let mut grand_memo_total = 0.0;

    for &(label, change_n) in scenarios {
        let frames = 1000;

        // ---- (A) no_cache: recompute everything every frame ----
        let start = Instant::now();
        for i in 0..frames {
            let p = if (i / change_n) % 2 == 0 {
                &params_a
            } else {
                &params_b
            };
            let f = filter_data(&data, p);
            let (m, p95) = aggregate(&f, AggregateMode::Median);
            let _ = format_results(&m, &p95);
        }
        let no_cache_total = start.elapsed();

        // ---- (B) manual_cache: version-checked, cascade invalidation ----
        //
        // This is what an experienced developer would write to avoid
        // recomputing every frame.  Each stage stores its previous inputs
        // and the cached output.  When an input changes, the cache for
        // that stage AND all downstream stages must be invalidated.
        //
        // For N stages this requires O(N²) invalidation logic in the
        // worst case, and it's easy to forget a cascade edge.
        let start = Instant::now();
        {
            let mut cached_params: Option<FilterParams> = None;
            let mut cached_filtered: Option<Vec<SaleRecord>> = None;
            let mut cached_mode: Option<AggregateMode> = None;
            let mut cached_agg: Option<(Vec<f64>, Vec<f64>)> = None;
            let mut cached_fmt_mode: Option<AggregateMode> = None;
            let mut cached_formatted: Option<String> = None;

            for i in 0..frames {
                let p = if (i / change_n) % 2 == 0 {
                    &params_a
                } else {
                    &params_b
                };

                // Stage 1: filter
                let filter_changed = cached_params.as_ref() != Some(p);
                if filter_changed {
                    cached_filtered = Some(filter_data(&data, p));
                    cached_params = Some(p.clone());
                }

                // Stage 2: aggregate (depends on filter output)
                let mode = AggregateMode::Median;
                let agg_changed = filter_changed || cached_mode != Some(mode);
                if agg_changed {
                    cached_agg = Some(aggregate(cached_filtered.as_ref().unwrap(), mode));
                    cached_mode = Some(mode);
                }

                // Stage 3: format (depends on aggregate output)
                let fmt_changed = agg_changed || cached_fmt_mode != Some(mode);
                if fmt_changed {
                    cached_formatted = Some(format_results(
                        &cached_agg.as_ref().unwrap().0,
                        &cached_agg.as_ref().unwrap().1,
                    ));
                    cached_fmt_mode = Some(mode);
                }

                let _ = cached_formatted.as_ref().unwrap();
            }
        }
        let manual_cache_total = start.elapsed();

        // ---- (C) auralis_memo: automatic dependency tracking ----
        let raw_sig = Signal::new(data.clone());
        let fp_sig = Signal::new(params_a.clone());
        let mode_s = Signal::new(AggregateMode::Median);

        let fm = auralis_signal::memo!(raw_sig, fp_sig =>
            filter_data(&raw_sig.read(), &fp_sig.read())
        );
        let am = auralis_signal::memo!(fm, mode_s =>
            aggregate(&fm.read(), mode_s.read())
        );
        let om = auralis_signal::memo!(am =>
            { let (m, p) = am.read(); format_results(&m, &p) }
        );

        let start = Instant::now();
        for i in 0..frames {
            if i % change_n == 0 {
                let new_p = if (i / change_n) % 2 == 0 {
                    params_a.clone()
                } else {
                    params_b.clone()
                };
                fp_sig.set_if_changed(new_p);
            }
            let _ = om.read();
        }
        let memo_total = start.elapsed();

        // Compute per-frame averages
        let nc_avg = no_cache_total.as_secs_f64() * 1000.0 / frames as f64;
        let mc_avg = manual_cache_total.as_secs_f64() * 1000.0 / frames as f64;
        let am_avg = memo_total.as_secs_f64() * 1000.0 / frames as f64;

        println!(
            "  {:<28} {:>7.2}ms/fr {:>10.2}ms/fr {:>10.2}ms/fr   (memo cache: {}% hits)",
            label,
            nc_avg,
            mc_avg,
            am_avg,
            (frames as usize - (fm.compute_count() as usize - 1)) * 100 / frames as usize,
        );
        let mc_ms = manual_cache_total.as_secs_f64() * 1000.0;
        let am_ms = memo_total.as_secs_f64() * 1000.0;
        grand_manual_total += mc_ms;
        grand_memo_total += am_ms;

        println!(
            "    Cumulative 1000fr total: manual={:.2}ms  auralis={:.2}ms  overhead={:.1}%",
            mc_ms,
            am_ms,
            (am_ms - mc_ms) / mc_ms * 100.0,
        );
    }

    println!(
        "\n  --- 3000-frame grand total ---\n  manual_cache: {:.2}ms  auralis_memo: {:.2}ms  overall overhead: {:.1}%",
        grand_manual_total,
        grand_memo_total,
        (grand_memo_total - grand_manual_total) / grand_manual_total * 100.0,
    );

    println!();

    // Micro-benchmark: single-access cost for each approach
    println!("--- Single-access cost (500K records, params unchanged) ---");

    // no_cache: full pipeline
    let start = Instant::now();
    let f = filter_data(&data, &params_a);
    let (m, p95) = aggregate(&f, AggregateMode::Median);
    let _ = format_results(&m, &p95);
    let nc_one = start.elapsed();

    // manual_cache: cache hit (all inputs same)
    let c_params = params_a.clone();
    let c_filtered = filter_data(&data, &c_params);
    let c_mode = AggregateMode::Median;
    let c_agg = aggregate(&c_filtered, c_mode);
    let c_fmt = format_results(&c_agg.0, &c_agg.1);
    let start = Instant::now();
    {
        let p = &params_a;
        let filter_changed = Some(p) != Some(&c_params);
        let mode = AggregateMode::Median;
        let agg_changed = filter_changed || Some(mode) != Some(c_mode);
        let fmt_changed = agg_changed || Some(mode) != Some(c_mode);
        let _ = if fmt_changed {
            format_results(&c_agg.0, &c_agg.1)
        } else {
            c_fmt.clone()
        };
    }
    let mc_one = start.elapsed();

    // auralis_memo: cache hit
    let rs = Signal::new(data.clone());
    let fs = Signal::new(params_a.clone());
    let ms = Signal::new(AggregateMode::Median);
    let fm = auralis_signal::memo!(rs, fs =>
        filter_data(&rs.read(), &fs.read())
    );
    let am = auralis_signal::memo!(fm, ms =>
        aggregate(&fm.read(), ms.read())
    );
    let om = auralis_signal::memo!(am =>
        { let (m, p) = am.read(); format_results(&m, &p) }
    );
    let _ = om.read(); // initial compute
    let start = Instant::now();
    let _ = om.read();
    let am_one = start.elapsed();

    println!(
        "  no_cache:      {:>8.2}ms  (full pipeline every access)",
        nc_one.as_secs_f64() * 1000.0,
    );
    println!(
        "  manual_cache:  {:>8.2}ms  (version-check cascade, 3-stage invalidation)",
        mc_one.as_secs_f64() * 1000.0,
    );
    println!(
        "  auralis_memo:  {:>8.2}ms  (auto dependency graph, no manual invalidation)",
        am_one.as_secs_f64() * 1000.0,
    );

    println!("\n--- Code complexity comparison (3-stage pipeline) ---");
    println!("  no_cache:      1 line per stage = 3 lines, always correct, always slow");
    println!(
        "  manual_cache:  ~20 lines of cache + invalidation logic, fragile to pipeline changes"
    );
    println!("  auralis_memo:  1 Memo::new per stage = 3 lines, always correct, optimally lazy");

    println!("\n=== Done ===");
}
