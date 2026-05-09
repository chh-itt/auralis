use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use auralis_signal::Signal;
use auralis_task::{CallbackHandle, Executor, TaskScope};
use egui::Ui;

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

type SourceId = u64;

#[derive(Clone, Debug)]
enum LoadProgress {
    Pending,
    Chunk(u32),
    Done(String),
    #[allow(dead_code)]
    Error(String),
}

struct Source {
    id: SourceId,
    name: &'static str,
    delay_ms: u64,
}

const SOURCES: &[Source] = &[
    Source {
        id: 0,
        name: "Sales DB",
        delay_ms: 800,
    },
    Source {
        id: 1,
        name: "Inventory API",
        delay_ms: 1200,
    },
    Source {
        id: 2,
        name: "Analytics Svc",
        delay_ms: 2000,
    },
    Source {
        id: 3,
        name: "User Profiles",
        delay_ms: 500,
    },
    Source {
        id: 4,
        name: "Log Stream",
        delay_ms: 3000,
    },
    Source {
        id: 5,
        name: "Config Server",
        delay_ms: 600,
    },
    Source {
        id: 6,
        name: "Metrics DB",
        delay_ms: 1500,
    },
    Source {
        id: 7,
        name: "Auth Service",
        delay_ms: 400,
    },
];

// ---------------------------------------------------------------------------
// Spawning helper (used by both sides for the actual I/O simulation)
// ---------------------------------------------------------------------------

fn spawn_thread(
    src: &Source,
    tx: mpsc::Sender<LoadProgress>,
    cancel: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let src_id = src.id;
    let src_name = src.name;
    let delay = src.delay_ms;

    std::thread::spawn(move || {
        for chunk in 0..10 {
            if cancel.load(Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(Duration::from_millis(delay / 10));
            let _ = tx.send(LoadProgress::Chunk(chunk));
        }
        if !cancel.load(Ordering::Relaxed) {
            let _ = tx.send(LoadProgress::Done(format!(
                "[{}] {} records loaded",
                src_name,
                (src_id + 1) * 1000
            )));
        }
    })
}

// ---------------------------------------------------------------------------
// Left panel: plain egui (manual thread management — done correctly)
// ---------------------------------------------------------------------------

pub struct WithoutAuralisLoader {
    handles: Vec<JoinHandle<()>>,
    /// Every spawned thread has its cancel flag stored here.
    /// 100% coverage — but must be maintained manually.
    cancel_flags: Vec<Arc<AtomicBool>>,
    results: Arc<Mutex<HashMap<SourceId, LoadProgress>>>,
    receivers: Vec<mpsc::Receiver<LoadProgress>>,
    loading: bool,
}

impl Default for WithoutAuralisLoader {
    fn default() -> Self {
        Self {
            handles: Vec::new(),
            cancel_flags: Vec::new(),
            results: Arc::new(Mutex::new(HashMap::new())),
            receivers: Vec::new(),
            loading: false,
        }
    }
}

impl WithoutAuralisLoader {
    fn start_load(&mut self) {
        self.cancel_all();
        self.loading = true;

        let results = Arc::clone(&self.results);
        {
            let mut map = results.lock().unwrap();
            for src in SOURCES {
                map.insert(src.id, LoadProgress::Pending);
            }
        }

        let mut handles = Vec::new();
        let mut cancel_flags = Vec::new();
        let mut receivers = Vec::new();

        for src in SOURCES {
            let (tx, rx) = mpsc::channel();
            let cancel = Arc::new(AtomicBool::new(false));

            // Store EVERY cancel flag — correct but manual discipline
            cancel_flags.push(Arc::clone(&cancel));

            handles.push(spawn_thread(src, tx, cancel));
            receivers.push(rx);
        }

        self.handles = handles;
        self.cancel_flags = cancel_flags;
        self.receivers = receivers;
    }

    fn cancel_all(&mut self) {
        // Must manually iterate ALL cancel flags — easy to forget one
        for flag in &self.cancel_flags {
            flag.store(true, Ordering::Relaxed);
        }
        self.handles.clear();
        self.cancel_flags.clear();
        self.receivers.clear();
        self.loading = false;
    }

    fn drain(&self) {
        let mut map = self.results.lock().unwrap();
        for (i, rx) in self.receivers.iter().enumerate() {
            let src_id = SOURCES[i].id;
            while let Ok(progress) = rx.try_recv() {
                map.insert(src_id, progress);
            }
        }
    }

    fn active_count(&self) -> usize {
        self.handles.len()
    }
}

// ---------------------------------------------------------------------------
// Right panel: Auralis (TaskScope + Signal + CallbackHandle)
// ---------------------------------------------------------------------------

pub struct WithAuralisLoader {
    pub executor: Rc<RefCell<Executor>>,
    root_scope: TaskScope,
    loading_scope: Option<TaskScope>,
    results: Signal<HashMap<SourceId, LoadProgress>>,
    receivers: Rc<RefCell<Vec<mpsc::Receiver<LoadProgress>>>>,
    loading: bool,
}

impl Default for WithAuralisLoader {
    fn default() -> Self {
        let mut results = HashMap::new();
        for src in SOURCES {
            results.insert(src.id, LoadProgress::Pending);
        }
        Self {
            executor: Executor::new_instance(),
            root_scope: TaskScope::new(),
            loading_scope: None,
            results: Signal::new(results),
            receivers: Rc::new(RefCell::new(Vec::new())),
            loading: false,
        }
    }
}

impl WithAuralisLoader {
    fn start_load(&mut self) {
        self.loading_scope = None;
        self.receivers.borrow_mut().clear();

        let mut map = HashMap::new();
        for src in SOURCES {
            map.insert(src.id, LoadProgress::Pending);
        }
        self.results.set(map);

        let scope = TaskScope::new_child(&self.root_scope);
        let receivers = Rc::clone(&self.receivers);

        for src in SOURCES {
            let (tx, rx) = mpsc::channel();
            receivers.borrow_mut().push(rx);

            let cancel = Arc::new(AtomicBool::new(false));
            let c = cancel.clone();
            // CallbackHandle in scope → auto-cancelled on scope drop
            scope.register_callback_handle(CallbackHandle::new(move || {
                c.store(true, Ordering::Relaxed);
            }));

            spawn_thread(src, tx, cancel);
        }

        self.loading_scope = Some(scope);
        self.loading = true;
    }

    fn cancel_all(&mut self) {
        // One line — scope drop cascades to ALL CallbackHandles via BFS
        self.loading_scope = None;
        self.receivers.borrow_mut().clear();
        self.loading = false;
    }

    fn drain(&self) {
        Executor::flush_instance(&self.executor);
        let mut map = self.results.read();
        let receivers = self.receivers.borrow();
        for (i, rx) in receivers.iter().enumerate() {
            let src_id = SOURCES[i].id;
            while let Ok(progress) = rx.try_recv() {
                map.insert(src_id, progress);
            }
        }
        self.results.set(map);
    }

    fn active_count(&self) -> usize {
        if self.loading_scope.is_some() {
            SOURCES.len()
        } else {
            0
        }
    }
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct LoaderState {
    pub without: WithoutAuralisLoader,
    pub with: WithAuralisLoader,
}

// ---------------------------------------------------------------------------
// UI
// ---------------------------------------------------------------------------

pub fn render_loader_ui(ui: &mut Ui, state: &mut LoaderState) {
    ui.horizontal(|ui| {
        if ui.button("Start Load").clicked() {
            state.without.start_load();
            state.with.start_load();
        }
        if ui.button("Cancel").clicked() {
            state.without.cancel_all();
            state.with.cancel_all();
        }
        ui.label(format!("Data sources: {}", SOURCES.len()));
    });

    ui.separator();

    state.without.drain();
    state.with.drain();

    ui.columns(2, |cols| {
        // ---- LEFT: manual ----
        cols[0].heading("Without Auralis");
        cols[0].label("(manual Vec<JoinHandle> + Vec<Arc<AtomicBool>>)");
        cols[0].separator();

        let active = state.without.active_count();
        cols[0].label(format!("Active handles: {}", active));

        // Show the cancel code that must be written
        cols[0].monospace(
            "// Cancel all — must iterate manually:\n\
             for flag in &self.cancel_flags {\n    \
                 flag.store(true, Ordering::Relaxed);\n\
             }\n\
             // ⚠ Adding a new source? Don't forget\n\
             //   to push its cancel flag to the Vec!",
        );

        let map = state.without.results.lock().unwrap();
        render_progress_bars(&mut cols[0], &map);

        // ---- RIGHT: Auralis ----
        cols[1].heading("With Auralis");
        cols[1].label("(TaskScope + Signal + CallbackHandle)");
        cols[1].separator();

        let active = state.with.active_count();
        cols[1].label(format!("Active tasks: {}", active));

        cols[1].monospace(
            "// Cancel all — one line:\n\
             self.loading_scope = None;\n\
             // ✓ Scope owns all CallbackHandles.\n\
             //   BFS iterates them all. Can't miss.",
        );

        let map = state.with.results.read();
        render_progress_bars(&mut cols[1], &map);
    });
}

fn render_progress_bars(ui: &mut Ui, results: &HashMap<SourceId, LoadProgress>) {
    egui::ScrollArea::vertical()
        .max_height(300.0)
        .show(ui, |ui| {
            for src in SOURCES {
                let progress = results
                    .get(&src.id)
                    .cloned()
                    .unwrap_or(LoadProgress::Pending);
                match progress {
                    LoadProgress::Pending => {
                        ui.label(format!("{}  [          ] pending", src.name));
                    }
                    LoadProgress::Chunk(n) => {
                        let bar = "█".repeat((n + 1) as usize);
                        let space = "░".repeat(10 - (n + 1) as usize);
                        ui.label(format!("{}  [{}{}] {}/10", src.name, bar, space, n + 1));
                    }
                    LoadProgress::Done(msg) => {
                        ui.colored_label(egui::Color32::GREEN, format!("{}  ✓ {}", src.name, msg));
                    }
                    LoadProgress::Error(e) => {
                        ui.colored_label(egui::Color32::RED, format!("{}  ✗ {}", src.name, e));
                    }
                }
            }
        });
}
