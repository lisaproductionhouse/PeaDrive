mod app;
mod config;
mod downloader;
mod drive_api;
mod oauth;

fn main() -> eframe::Result {
    // Không có logger thì lỗi ở tầng dựng cửa sổ/đồ họa sẽ âm thầm không in
    // ra gì cả, nên cài log ở đây để dễ chẩn đoán nếu app không mở lên được.
    env_logger::init();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([780.0, 760.0])
            .with_min_inner_size([480.0, 420.0])
            .with_title("Sao chép thư mục Google Drive công khai"),
        ..Default::default()
    };

    eframe::run_native(
        "Sao chép thư mục Google Drive công khai",
        options,
        Box::new(|cc| Ok(Box::new(app::GDriveCopierApp::new(cc)))),
    )
}
