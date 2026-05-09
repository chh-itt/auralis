use egui::Ui;

use crate::data_gen::{generate_sales, SaleRecord, CATEGORY_NAMES, REGION_NAMES};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

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

#[derive(Clone, Copy, PartialEq, Debug)]
enum AggregateMode {
    Sum,
    Median,
    P95,
}

impl AggregateMode {
    fn name(self) -> &'static str {
        match self {
            Self::Sum => "Sum",
            Self::Median => "Median",
            Self::P95 => "P95",
        }
    }
}

#[derive(Clone)]
struct RegionStats {
    name: &'static str,
    count: usize,
    sum: f64,
    median: f64,
    p95: f64,
}

#[derive(Clone)]
struct StatsResult {
    regions: Vec<RegionStats>,
    total_count: usize,
}

// ---------------------------------------------------------------------------
// Computation pipeline
// ---------------------------------------------------------------------------

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

fn aggregate(records: &[SaleRecord], _mode: AggregateMode) -> StatsResult {
    let n_regions = REGION_NAMES.len();
    let mut buckets: Vec<Vec<f64>> = vec![Vec::new(); n_regions];
    for r in records {
        buckets[r.region as usize].push(r.amount);
    }
    let mut regions: Vec<RegionStats> = Vec::with_capacity(n_regions);
    let mut total_count = 0;
    for (i, amounts) in buckets.iter_mut().enumerate() {
        if amounts.is_empty() {
            regions.push(RegionStats {
                name: REGION_NAMES[i],
                count: 0,
                sum: 0.0,
                median: 0.0,
                p95: 0.0,
            });
            continue;
        }
        amounts.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        let count = amounts.len();
        total_count += count;
        let sum: f64 = amounts.iter().sum();
        let median = percentile(amounts, 0.5);
        let p95 = percentile(amounts, 0.95);
        regions.push(RegionStats {
            name: REGION_NAMES[i],
            count,
            sum,
            median,
            p95,
        });
    }
    StatsResult {
        regions,
        total_count,
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn format_results(result: &StatsResult, mode: AggregateMode) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "Total matched: {} records\n\n",
        result.total_count
    ));
    out.push_str("Region      Count        ");
    match mode {
        AggregateMode::Sum => out.push_str("Sum\n"),
        AggregateMode::Median => out.push_str("Median\n"),
        AggregateMode::P95 => out.push_str("P95\n"),
    }
    out.push_str("──────────  ──────────  ──────────\n");
    for r in &result.regions {
        let val = match mode {
            AggregateMode::Sum => format!("${:.2}", r.sum),
            AggregateMode::Median => format!("${:.2}", r.median),
            AggregateMode::P95 => format!("${:.2}", r.p95),
        };
        out.push_str(&format!("{:<12}{:>10}  {}\n", r.name, r.count, val));
    }
    out
}

// ---------------------------------------------------------------------------
// Left panel: manual cache (version-checked, cascade invalidation)
// ---------------------------------------------------------------------------
//
// This is what an experienced developer writes to avoid recomputing
// every frame.  It is correct and efficient — but the invalidation
// cascade must be maintained by hand.  Adding a pipeline stage means
// updating 2-3 condition checks and remembering to invalidate all
// downstream caches.

pub struct WithoutAuralisAnalyzer {
    raw_data: Vec<SaleRecord>,

    // Filter cache
    cached_filter_params: FilterParams,
    cached_filtered: Vec<SaleRecord>,

    // Aggregate cache
    cached_agg_mode: AggregateMode,
    cached_agg: StatsResult,

    // Format cache
    cached_fmt_mode: AggregateMode,
    cached_formatted: String,
}

impl WithoutAuralisAnalyzer {
    fn new(data_size: usize) -> Self {
        let raw = generate_sales(data_size);
        let fp = FilterParams::default();
        let mode = AggregateMode::Median;
        let filtered = filter_data(&raw, &fp);
        let agg = aggregate(&filtered, mode);
        let formatted = format_results(&agg, mode);

        Self {
            raw_data: raw,
            cached_filter_params: fp,
            cached_filtered: filtered,
            cached_agg_mode: mode,
            cached_agg: agg,
            cached_fmt_mode: mode,
            cached_formatted: formatted,
        }
    }

    fn compute(&mut self, params: &FilterParams, mode: AggregateMode) -> &str {
        // Stage 1: filter (depends on raw_data + filter_params)
        let filter_changed = *params != self.cached_filter_params;
        if filter_changed {
            self.cached_filtered = filter_data(&self.raw_data, params);
            self.cached_filter_params = params.clone();
        }

        // Stage 2: aggregate (depends on filtered data + mode)
        let agg_changed = filter_changed || mode != self.cached_agg_mode;
        if agg_changed {
            self.cached_agg = aggregate(&self.cached_filtered, mode);
            self.cached_agg_mode = mode;
        }

        // Stage 3: format (depends on aggregate result + mode)
        let fmt_changed = agg_changed || mode != self.cached_fmt_mode;
        if fmt_changed {
            self.cached_formatted = format_results(&self.cached_agg, mode);
            self.cached_fmt_mode = mode;
        }

        &self.cached_formatted
    }

    fn resize(&mut self, size: usize) {
        self.raw_data = generate_sales(size);
        // Invalidate all caches
        self.cached_filter_params = FilterParams {
            date_start: 999,
            ..FilterParams::default()
        };
    }
}

// ---------------------------------------------------------------------------
// Right panel: Auralis (Signal + Memo chain)
// ---------------------------------------------------------------------------

use auralis_signal::{Memo, Signal};

pub struct WithAuralisAnalyzer {
    raw_data: Signal<Vec<SaleRecord>>,
    filter_params: Signal<FilterParams>,
    aggregate_mode: Signal<AggregateMode>,
    filtered_data: Memo<Vec<SaleRecord>>,
    aggregated_result: Memo<StatsResult>,
    formatted_output: Memo<String>,
}

impl WithAuralisAnalyzer {
    fn new(data_size: usize) -> Self {
        let raw_data = Signal::new(generate_sales(data_size));
        let filter_params = Signal::new(FilterParams::default());
        let aggregate_mode = Signal::new(AggregateMode::Median);

        let filtered_data = auralis_signal::memo!(raw_data, filter_params =>
            filter_data(&raw_data.read(), &filter_params.read())
        );

        let aggregated_result = auralis_signal::memo!(filtered_data, aggregate_mode =>
            aggregate(&filtered_data.read(), aggregate_mode.read())
        );

        let formatted_output = auralis_signal::memo!(aggregated_result, aggregate_mode =>
            format_results(&aggregated_result.read(), aggregate_mode.read())
        );

        Self {
            raw_data,
            filter_params,
            aggregate_mode,
            filtered_data,
            aggregated_result,
            formatted_output,
        }
    }

    fn set_filter(&self, params: FilterParams) {
        self.filter_params.set(params);
    }

    fn set_mode(&self, mode: AggregateMode) {
        self.aggregate_mode.set(mode);
    }

    fn resize(&self, size: usize) {
        self.raw_data.set(generate_sales(size));
    }

    fn compute_result(&self) -> String {
        self.formatted_output.read()
    }

    fn filter_dirty(&self) -> bool {
        self.filtered_data.is_dirty()
    }
    fn agg_dirty(&self) -> bool {
        self.aggregated_result.is_dirty()
    }
    fn fmt_dirty(&self) -> bool {
        self.formatted_output.is_dirty()
    }
    fn filter_cc(&self) -> u64 {
        self.filtered_data.compute_count()
    }
    fn agg_cc(&self) -> u64 {
        self.aggregated_result.compute_count()
    }
    fn fmt_cc(&self) -> u64 {
        self.formatted_output.compute_count()
    }
}

// ---------------------------------------------------------------------------
// Shared state for Tab 2
// ---------------------------------------------------------------------------

pub struct AnalyzerState {
    pub without: WithoutAuralisAnalyzer,
    pub with: WithAuralisAnalyzer,
    data_size: usize,
}

impl Default for AnalyzerState {
    fn default() -> Self {
        Self {
            without: WithoutAuralisAnalyzer::new(500_000),
            with: WithAuralisAnalyzer::new(500_000),
            data_size: 500_000,
        }
    }
}

impl AnalyzerState {
    pub fn resize_if_needed(&mut self, new_size: usize) {
        if new_size != self.data_size {
            self.data_size = new_size;
            self.without.resize(new_size);
            self.with.resize(new_size);
        }
    }
}

// ---------------------------------------------------------------------------
// UI
// ---------------------------------------------------------------------------

const DATA_SIZE_OPTIONS: &[usize] = &[100_000, 250_000, 500_000, 1_000_000];

pub fn render_analyzer_ui(ui: &mut Ui, state: &mut AnalyzerState) {
    let mut filter = FilterParams::default();
    let mut mode = AggregateMode::Median;
    let mut new_size = state.data_size;

    ui.horizontal(|ui| {
        ui.label("Date range:");
        ui.add(
            egui::Slider::new(&mut filter.date_start, 0..=365)
                .text("start")
                .step_by(1.0),
        );
        ui.add(
            egui::Slider::new(&mut filter.date_end, 0..=365)
                .text("end")
                .step_by(1.0),
        );
    });

    ui.horizontal(|ui| {
        ui.label("Categories:");
        ui.horizontal_wrapped(|ui| {
            for (i, name) in CATEGORY_NAMES.iter().enumerate() {
                ui.toggle_value(&mut filter.categories[i], *name);
            }
        });
    });

    ui.horizontal(|ui| {
        ui.label("Aggregate:");
        egui::ComboBox::from_id_salt("agg_mode")
            .selected_text(mode.name())
            .show_ui(ui, |ui| {
                ui.selectable_value(&mut mode, AggregateMode::Sum, "Sum");
                ui.selectable_value(&mut mode, AggregateMode::Median, "Median");
                ui.selectable_value(&mut mode, AggregateMode::P95, "P95");
            });

        ui.separator();

        ui.label("Data size:");
        for &size in DATA_SIZE_OPTIONS {
            ui.selectable_value(&mut new_size, size, format!("{}K", size / 1000));
        }
    });

    state.resize_if_needed(new_size);

    ui.separator();

    ui.columns(2, |cols| {
        // ---- LEFT: manual cache ----
        cols[0].heading("Without Auralis");
        cols[0].label("(manual cache with version-check cascade)");
        cols[0].separator();

        let start = std::time::Instant::now();
        let left_result = state.without.compute(&filter, mode).to_string();
        let left_elapsed = start.elapsed().as_secs_f64() * 1000.0;

        cols[0].label(format!("Compute time: {:.2} ms", left_elapsed));
        cols[0].label("Code: ~20 lines of invalidation logic");
        cols[0].monospace(
            "// Manual cascade — must be maintained:\n\
             let filter_changed = params != cache;\n\
             if filter_changed { recompute; }\n\
             let agg_changed = filter_changed ||\n    mode != cached_mode;\n\
             if agg_changed { recompute; }\n\
             let fmt_changed = agg_changed ||\n    mode != cached_fmt_mode;",
        );

        egui::ScrollArea::vertical()
            .max_height(350.0)
            .show(&mut cols[0], |ui| {
                ui.monospace(&left_result);
            });

        // ---- RIGHT: Auralis Memo ----
        cols[1].heading("With Auralis");
        cols[1].label("(Memo chain — automatic dependency tracking)");
        cols[1].separator();

        state.with.set_filter(filter);
        state.with.set_mode(mode);

        let start = std::time::Instant::now();
        let right_result = state.with.compute_result();
        let right_elapsed = start.elapsed().as_secs_f64() * 1000.0;

        cols[1].label(format!("Compute time: {:.2} ms", right_elapsed));
        cols[1].label("Code: 1 Memo::new per stage = 3 lines");
        cols[1].monospace(
            "// Auto dependency tracking:\n\
             Memo::new(|| filter(..));\n\
             Memo::new(|| aggregate(..));\n\
             Memo::new(|| format(..));\n\
             // No invalidation code at all",
        );

        cols[1].label(format!(
            "Dirty: filter={} agg={} fmt={}",
            state.with.filter_dirty(),
            state.with.agg_dirty(),
            state.with.fmt_dirty(),
        ));
        cols[1].label(format!(
            "Compute#: filter={} agg={} fmt={}",
            state.with.filter_cc(),
            state.with.agg_cc(),
            state.with.fmt_cc(),
        ));

        egui::ScrollArea::vertical()
            .max_height(350.0)
            .show(&mut cols[1], |ui| {
                ui.monospace(&right_result);
            });
    });
}
