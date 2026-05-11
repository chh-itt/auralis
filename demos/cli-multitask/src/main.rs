use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use auralis_signal::Signal;
use auralis_task::{Executor, TaskScope};

// ---------------------------------------------------------------------------
// Progress display
// ---------------------------------------------------------------------------

fn print_status(workers: &[(usize, u32, u32)]) {
    print!("\r  ");
    for &(id, done, total) in workers {
        let bar_w = 10;
        let filled = (done as usize * bar_w / total as usize).min(bar_w);
        print!(
            "[{}{}] w{id}: {done}/{total}  ",
            "#".repeat(filled),
            ".".repeat(bar_w - filled),
        );
    }
    print!("\r");
}

// ---------------------------------------------------------------------------
// Background worker (no Auralis — pure std::thread + channel)
// ---------------------------------------------------------------------------

fn spawn_worker(
    id: usize,
    total_chunks: u32,
    delay_ms: u64,
    tx: mpsc::Sender<(usize, u32)>,
    cancel: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        for chunk in 0..total_chunks {
            if cancel.load(Ordering::Relaxed) {
                eprintln!("\n  worker {id}: cancelled at chunk {chunk}/{total_chunks}");
                return;
            }
            std::thread::sleep(Duration::from_millis(delay_ms));
            tx.send((id, chunk + 1)).ok();
        }
        eprintln!("\n  worker {id}: done");
    })
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    println!("=== Auralis CLI Multi-Task Demo ===\n");
    println!("Spawning 4 simulated workers. Press Ctrl+C to cancel all.\n");

    // ---- worker config ----
    let workers: Vec<(usize, u32, u32)> = vec![
        (0, 40, 100),  // ~4s
        (1, 80, 100),  // ~8s
        (2, 30, 200),  // ~6s
        (3, 60, 200),  // ~12s
    ];

    // ---- channel for thread → main communication ----
    let (tx, rx) = mpsc::channel();

    // ---- Auralis executor + scope ----
    let ex = Executor::new_instance();
    let mut root = Some(TaskScope::with_executor(&ex));
    let root_ref = root.as_ref().unwrap();

    // ---- progress state (owned by main thread) ----
    let progress = Signal::new(
        workers
            .iter()
            .map(|&(i, _, total)| (i, 0u32, total))
            .collect::<Vec<_>>(),
    );

    // ---- cancel flags + spawn threads ----
    let cancel_flags: Vec<Arc<AtomicBool>> = workers.iter().map(|_| Arc::new(AtomicBool::new(false))).collect();
    let mut handles = Vec::new();

    for &(id, _, chunks) in &workers {
        let flag = Arc::clone(&cancel_flags[id]);
        let tx2 = tx.clone();
        let delay = match id { 0..=1 => 100, _ => 200 };
        handles.push(spawn_worker(id, chunks, delay, tx2, flag));
    }

    // ---- register cancel flags in root scope → drop fires all ----
    for flag in &cancel_flags {
        let f = Arc::clone(flag);
        root_ref.on_cleanup(move || {
            f.store(true, Ordering::Relaxed);
        });
    }

    // ---- Ctrl+C handler (runs on signal thread — must be Send) ----
    let cancel_signal = Arc::new(AtomicBool::new(false));
    let cs = Arc::clone(&cancel_signal);
    ctrlc::set_handler(move || {
        eprintln!("\nCtrl+C received — cancelling all tasks...");
        cs.store(true, Ordering::Relaxed);
    })
    .expect("failed to set Ctrl+C handler");

    // ---- main loop ----
    let start = std::time::Instant::now();
    loop {
        // Drain channel → update signal on main thread.
        while let Ok((id, done)) = rx.try_recv() {
            let mut state = progress.read();
            if let Some(entry) = state.iter_mut().find(|(i, _, _)| *i == id) {
                entry.1 = done;
            }
            progress.set(state);
        }

        // Flush the Auralis executor.
        Executor::flush_instance(&ex);

        // Display.
        let state = progress.read();
        print_status(&state);

        // Check if all done.
        if state.iter().all(|(_, done, total)| done >= total) {
            println!("\n\nAll workers completed in {:.1}s.", start.elapsed().as_secs_f64());
            break;
        }

        // Check if Ctrl+C was pressed — drop root scope on main thread.
        if cancel_signal.load(Ordering::Relaxed) {
            drop(root.take());
            // Give threads a moment to observe the cancel flag.
            std::thread::sleep(Duration::from_millis(200));
            println!("All workers cancelled in {:.1}s.", start.elapsed().as_secs_f64());
            break;
        }

        std::thread::sleep(Duration::from_millis(50));
    }
    // Ensure scope is dropped.
    drop(root);

    for h in handles {
        h.join().ok();
    }
}
