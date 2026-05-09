mod app;
mod async_loader;
mod data_analyzer;
mod data_gen;

use app::AuralisEguiDemo;

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Auralis + egui — Reactive Kernel Demo",
        options,
        Box::new(|_cc| Ok(Box::new(AuralisEguiDemo::default()))),
    )
}
