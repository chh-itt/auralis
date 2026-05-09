use std::collections::VecDeque;

use eframe::Frame;
use egui::{Context, Ui};

use crate::async_loader::{self, LoaderState};
use crate::data_analyzer::{self, AnalyzerState};

// ---------------------------------------------------------------------------
// Tab
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    AsyncLoader,
    DataAnalyzer,
}

impl Tab {
    fn name(self) -> &'static str {
        match self {
            Self::AsyncLoader => "Async Loader",
            Self::DataAnalyzer => "Data Analyzer",
        }
    }
}

// ---------------------------------------------------------------------------
// FPS Tracker
// ---------------------------------------------------------------------------

struct FpsTracker {
    frame_times: VecDeque<f64>,
    last_time: Option<f64>,
}

impl FpsTracker {
    fn new() -> Self {
        Self {
            frame_times: VecDeque::with_capacity(120),
            last_time: None,
        }
    }

    fn update(&mut self, current_time: f64) {
        if let Some(prev) = self.last_time {
            let delta = current_time - prev;
            self.frame_times.push_back(delta);
            if self.frame_times.len() > 120 {
                self.frame_times.pop_front();
            }
        }
        self.last_time = Some(current_time);
    }

    fn fps(&self) -> f64 {
        let len = self.frame_times.len();
        if len < 2 {
            return 0.0;
        }
        let avg: f64 = self.frame_times.iter().sum::<f64>() / len as f64;
        if avg > 0.0 {
            1.0 / avg
        } else {
            0.0
        }
    }

    fn avg_frame_ms(&self) -> f64 {
        let len = self.frame_times.len();
        if len < 2 {
            return 0.0;
        }
        self.frame_times.iter().sum::<f64>() / len as f64 * 1000.0
    }
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

pub struct AuralisEguiDemo {
    current_tab: Tab,
    fps: FpsTracker,
    loader_state: LoaderState,
    analyzer_state: AnalyzerState,
}

impl Default for AuralisEguiDemo {
    fn default() -> Self {
        Self {
            current_tab: Tab::DataAnalyzer,
            fps: FpsTracker::new(),
            loader_state: LoaderState::default(),
            analyzer_state: AnalyzerState::default(),
        }
    }
}

impl eframe::App for AuralisEguiDemo {
    fn update(&mut self, ctx: &Context, _frame: &mut Frame) {
        // FPS tracking
        let now = ctx.input(|i| i.time);
        self.fps.update(now);

        // Request continuous repaint for FPS tracking and progress updates
        ctx.request_repaint();
    }

    fn ui(&mut self, ui: &mut Ui, _frame: &mut Frame) {
        // Tab bar
        egui::Panel::top("tab_bar").show_inside(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Auralis + egui — Reactive Kernel Demo");
                ui.separator();
                ui.selectable_value(
                    &mut self.current_tab,
                    Tab::DataAnalyzer,
                    Tab::DataAnalyzer.name(),
                );
                ui.selectable_value(
                    &mut self.current_tab,
                    Tab::AsyncLoader,
                    Tab::AsyncLoader.name(),
                );
                ui.separator();
                ui.label(format!(
                    "FPS: {:.0}  |  avg frame: {:.2} ms",
                    self.fps.fps(),
                    self.fps.avg_frame_ms(),
                ));
            });
        });

        // Main content
        egui::CentralPanel::default().show_inside(ui, |ui| {
            match self.current_tab {
                Tab::AsyncLoader => {
                    ui.heading("Async Data Loading — Lifecycle Management Comparison");
                    ui.label(
                        "8 simulated data sources with random delays. \
                         Click 'Start Load' to begin, 'Cancel' to abort.",
                    );
                    ui.label(
                        "Left: manual Vec<JoinHandle> + Vec<Arc<AtomicBool>> (correct, but manual discipline).\n\
                         Right: TaskScope + Signal + CallbackHandle (ownership guarantees cleanup).",
                    );
                    ui.separator();
                    async_loader::render_loader_ui(ui, &mut self.loader_state);
                }
                Tab::DataAnalyzer => {
                    ui.heading("Data Analysis Pipeline — Memo Lazy Recompute Comparison");
                    ui.label(
                        "500K sales records, 3-stage pipeline: filter → aggregate → format. \
                         Each stage tracks dependencies automatically with Memo.",
                    );
                    ui.label(
                        "Left: manual cache with version-check cascade (experienced dev's best effort).\n\
                         Right: Signal + Memo chain — automatic dependency tracking, no invalidation code.",
                    );
                    ui.separator();
                    data_analyzer::render_analyzer_ui(ui, &mut self.analyzer_state);
                }
            }
        });
    }
}
