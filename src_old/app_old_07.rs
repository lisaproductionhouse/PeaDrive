//! Giao diện người dùng (egui/eframe). File này chỉ lo việc HIỂN THỊ và điều
//! phối: mọi việc gọi mạng thật sự nằm ở `drive_api.rs` / `downloader.rs`,
//! chạy trên 1 tokio runtime nền, kết quả gửi về đây qua kênh (channel) rồi
//! mới cập nhật lên state để vẽ lại UI ở khung hình kế tiếp.
//!
//! Bố cục tối ưu cho màn hình rộng — chia theo chiều NGANG thay vì xếp dọc:
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────┐
//! │ [ link Drive ............................. ] [Mở]   ✔ mail  ⚙ │ thanh trên
//! ├─────────────────────────────────────────────┬──────────────────┤
//! │ ⬆ ⟳  Gốc › Thư mục › ...                    │ Tiến độ          │
//! │ [banner thông báo / xác nhận xóa]           │ ████░░  12/50    │
//! │ 120 mục · đã chọn 3    [Tải tất cả][Tải …]  │ 1.2/4.5 GB ...   │
//! │ ☐ Tên                 Dung lượng            │ [Nhật ký|Tải/Xóa]│
//! │ ☐ 📁 Thư mục A        xem dung lượng   Tải  │  ✔ a.jpg         │
//! │ ☑ 🎬 clip.mp4         1.2 GB      Tải ✎ 🗑  │  ✘ b.mp4: lỗi    │
//! ├─────────────────────────────────────────────┴──────────────────┤
//! │ Lưu vào: D:\Tai-ve [Chọn...] [Mở]    Nếu trùng tên: [Bỏ qua ▾] │ thanh dưới
//! └────────────────────────────────────────────────────────────────┘
//! ```
//!
//! - Thanh trên: link + tài khoản + cài đặt (luôn thấy).
//! - Vùng giữa: đường dẫn, thông báo, bảng file (chiếm gần hết cửa sổ).
//! - Khung phải (kéo giãn được): tiến độ tải, rồi tab Nhật ký / Tải-Xóa hàng loạt.
//! - Thanh dưới: nơi lưu + cách xử lý trùng tên.

use crate::config::{AppConfig, ConflictPolicy};
use crate::downloader::{self, WorkerEvent};
use crate::drive_api::{build_shared_http_client, extract_drive_id, DriveClient, DriveEditClient, DriveEntry};
use crate::oauth::OAuthTokens;
use futures_util::stream::{self, StreamExt};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use egui::{pos2, vec2, Align, Color32, Layout, Rect, RichText, Sense};

/// Trạng thái tính dung lượng của 1 thư mục — chỉ tính khi người dùng bấm
/// xem (xem `GDriveCopierApp::start_compute_folder_size`).
#[derive(Clone)]
enum FolderSizeState {
    Computing,
    Known { files: usize, bytes: u64 },
    Error,
}

/// Tên thư mục dùng làm nơi "gom" các file KHÔNG do tài khoản đang đăng
/// nhập sở hữu khi xóa (Google chỉ cho phép CHỦ SỞ HỮU chuyển thẳng vào
/// Thùng rác — xem `drive_api::WriteOperation::Trash`) — tạo ngay bên trong
/// thư mục cha của từng file, để chủ sở hữu dễ tìm thấy khi họ ghé qua.
///
/// LƯU Ý: nếu vẫn gặp lỗi 403 "Increasing the number of parents is not
/// allowed" khi chuyển, KHÔNG phải lỗi logic — nguyên nhân thường là chủ sở
/// hữu đang chia sẻ thư mục kiểu "Bất kỳ ai có đường liên kết" (Anyone with
/// the link), và Google giới hạn quyền `removeParents` (gỡ file khỏi thư
/// mục cha) cho kiểu chia sẻ này dù đã là "Người chỉnh sửa". Cách khắc
/// phục: nhờ chủ sở hữu chia sẻ TRỰC TIẾP tới đúng email tài khoản đang
/// đăng nhập trong app (thay vì chỉ chia sẻ qua link) — đã kiểm chứng thực
/// tế là chuyển được bình thường sau khi đổi cách chia sẻ.
const QUARANTINE_FOLDER_NAME: &str = "_Đã đánh dấu xóa (chờ chủ sở hữu xóa hẳn)";

/// Trạng thái của banner xác nhận xóa 1 file — luôn kiểm tra chủ sở hữu
/// TRƯỚC khi hỏi xác nhận, vì Google chỉ cho phép CHỦ SỞ HỮU chuyển thẳng
/// vào Thùng rác (kể cả có quyền Editor qua link cũng không đủ), nên phải
/// biết trước để hỏi đúng câu hỏi thay vì để người dùng gặp lỗi 403 rồi mới
/// biết.
#[derive(Clone)]
enum ConfirmDeleteState {
    CheckingOwnership(DriveEntry),
    /// KHÔNG phải chủ sở hữu — cần hỏi trước khi chuyển sang thư mục riêng.
    /// Nếu LÀ chủ sở hữu, không có trạng thái chờ xác nhận nào cả: xóa
    /// thẳng vào Thùng rác ngay khi biết kết quả kiểm tra (xem
    /// `WorkerEvent::OwnershipChecked`), đỡ phải hỏi thêm 1 bước cho
    /// trường hợp đơn giản/phổ biến nhất.
    NotOwned {
        entry: DriveEntry,
        owner_label: Option<String>,
        parent_id: Option<String>,
    },
}

enum JobState {
    Idle,
    Loading,
    Scanning {
        found: usize,
    },
    Downloading {
        total_files: usize,
        total_bytes: u64,
        done_files: usize,
        done_bytes: u64,
        last_started_name: Option<String>,
        /// Tốc độ tải hiện tại (byte/giây), đã làm mượt nhẹ để không nhảy
        /// giật cục giữa các khung hình.
        speed_bps: f64,
        speed_sample_at: std::time::Instant,
        speed_sample_bytes: u64,
    },
    /// Đang xóa hàng loạt (chuyển vào Thùng rác) theo danh sách tên.
    BulkDeleting {
        done: usize,
        total: usize,
    },
}

/// Tab đang mở ở khung bên phải.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ActivityTab {
    Log,
    BulkList,
}

pub struct GDriveCopierApp {
    config: AppConfig,
    /// 1 `reqwest::Client` DÙNG CHUNG cho mọi lời gọi Drive API + OAuth
    /// trong suốt vòng đời app — xem `drive_api::build_shared_http_client`.
    /// `.clone()` ở đây chỉ tăng refcount Arc bên trong, không dựng lại
    /// pool kết nối, nên có thể clone thoải mái mỗi khi cần đưa vào 1 tác
    /// vụ nền (spawn) mà không lo tốn kém.
    http: reqwest::Client,
    api_key_input: String,
    show_settings: bool,

    link_input: String,
    /// Ngăn xếp breadcrumb: mỗi phần tử là (folder_id, tên hiển thị).
    breadcrumbs: Vec<(String, String)>,
    current_entries: Vec<DriveEntry>,
    /// Dung lượng các thư mục đã tính (theo folder_id), giữ nguyên khi
    /// chuyển qua lại giữa các thư mục nên không phải tính lại.
    folder_sizes: HashMap<String, FolderSizeState>,
    /// ID các mục đang được tick chọn trong thư mục hiện tại — bị xóa mỗi
    /// khi danh sách thay đổi (điều hướng sang thư mục khác).
    selected: HashSet<String>,
    /// Vị trí (index trong danh sách đang hiển thị) của lần tick gần nhất
    /// KHÔNG giữ Shift — dùng làm điểm neo cho thao tác Shift+tick chọn cả
    /// khoảng.
    selection_anchor: Option<usize>,

    destination: Option<PathBuf>,
    /// Danh sách đang chờ tải vì lúc bấm "Tải" chưa có thư mục đích — sẽ
    /// được tự động tải tiếp ngay khi người dùng chọn xong thư mục từ hộp
    /// thoại (xem `WorkerEvent::DestinationPicked`).
    pending_download: Option<Vec<DriveEntry>>,

    job: JobState,
    cancel_flag: Arc<AtomicBool>,
    log: VecDeque<String>,
    status_message: Option<(String, bool)>,

    // --- Đăng nhập Google (OAuth) — chỉ cần cho đổi tên/xóa, KHÔNG liên
    // quan tới việc tải file (vẫn dùng API key, không cần đăng nhập) ---
    oauth_client_id_input: String,
    oauth_client_secret_input: String,
    login_busy: bool,

    /// (entry_id, tên đang gõ dở) của dòng đang được đổi tên tại chỗ.
    renaming: Option<(String, String)>,
    /// File đang chờ xác nhận trước khi thật sự chuyển vào Thùng rác — bao
    /// gồm cả bước đang kiểm tra chủ sở hữu (xem `ConfirmDeleteState`).
    confirm_delete: Option<ConfirmDeleteState>,
    /// Đồng ý cho chuyển vào thư mục chuẩn bị xóa hay không, khi
    /// `confirm_delete` đang ở trạng thái `NotOwned` — mặc định `true`.
    confirm_delete_move_aside: bool,

    /// Ô nhập danh sách tên file — DÙNG CHUNG cho TẢI hàng loạt và xóa hàng
    /// loạt (xem `draw_bulk_list_section`). Được giữ nguyên khi chuyển sang
    /// thư mục khác, để bấm "Tìm" lại cùng danh sách ở thư mục mới.
    bulk_list_input: String,
    /// Sau khi bấm "Tìm", danh sách các mục KHỚP tên đang hiển thị để xem
    /// trước, cùng các tên nhập vào KHÔNG khớp mục nào (để minh bạch, biết
    /// ngay tên nào cần kiểm tra lại).
    bulk_list_matches: Vec<DriveEntry>,
    bulk_list_unmatched: Vec<String>,
    /// Đồng ý cho CHUYỂN các file KHÔNG do mình sở hữu vào thư mục chuẩn bị
    /// xóa hay không — hỏi 1 LẦN DUY NHẤT trước khi bắt đầu (không hỏi lại
    /// giữa chừng theo từng file), mặc định `true` để đỡ thao tác vì rủi ro
    /// thấp (file luôn còn trong Thùng rác hoặc thư mục tạm, không mất gì).
    /// File do MÌNH sở hữu luôn được xóa thẳng, không phụ thuộc cờ này.
    bulk_delete_move_aside_agreed: bool,

    /// Tab đang mở ở khung bên phải (Nhật ký / Tải-Xóa hàng loạt).
    activity_tab: ActivityTab,
    /// `true` ngay sau khi bấm ✎ — ô đổi tên xin focus 1 lần ở khung hình kế tiếp.
    rename_focus: bool,

    runtime: tokio::runtime::Runtime,
    event_tx: UnboundedSender<WorkerEvent>,
    event_rx: UnboundedReceiver<WorkerEvent>,
}

impl GDriveCopierApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        setup_vietnamese_font(&cc.egui_ctx);
        setup_style(&cc.egui_ctx);

        let config = AppConfig::load();
        let http = build_shared_http_client()
            .expect("Không dựng được HTTP client dùng chung cho Drive API");
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = tokio::runtime::Runtime::new().expect("Không khởi tạo được tokio runtime");
        let destination = config.last_destination.clone();
        let show_settings = config.api_key.is_none();
        let oauth_client_id_input = config.oauth_client_id.clone().unwrap_or_default();
        let oauth_client_secret_input = config.oauth_client_secret.clone().unwrap_or_default();
        Self {
            http,
            api_key_input: config.api_key.clone().unwrap_or_default(),
            show_settings,
            config,
            link_input: String::new(),
            breadcrumbs: Vec::new(),
            current_entries: Vec::new(),
            folder_sizes: HashMap::new(),
            selected: HashSet::new(),
            selection_anchor: None,
            destination,
            pending_download: None,
            job: JobState::Idle,
            cancel_flag: Arc::new(AtomicBool::new(false)),
            log: VecDeque::new(),
            status_message: None,
            oauth_client_id_input,
            oauth_client_secret_input,
            login_busy: false,
            renaming: None,
            confirm_delete: None,
            confirm_delete_move_aside: true,
            bulk_list_input: String::new(),
            bulk_list_matches: Vec::new(),
            bulk_list_unmatched: Vec::new(),
            bulk_delete_move_aside_agreed: true,
            activity_tab: ActivityTab::Log,
            rename_focus: false,
            runtime,
            event_tx,
            event_rx,
        }
    }

    fn is_busy(&self) -> bool {
        !matches!(self.job, JobState::Idle)
    }

    /// Tính lại tốc độ tải (byte/giây) theo mẫu ~1 lần mỗi 300ms, làm mượt
    /// nhẹ (EMA) để số hiển thị không nhảy giật cục giữa các khung hình.
    fn update_speed_estimate(&mut self) {
        if let JobState::Downloading {
            done_bytes,
            speed_bps,
            speed_sample_at,
            speed_sample_bytes,
            ..
        } = &mut self.job
        {
            let elapsed = speed_sample_at.elapsed().as_secs_f64();
            if elapsed >= 0.3 {
                let delta_bytes = done_bytes.saturating_sub(*speed_sample_bytes) as f64;
                let instant_speed = delta_bytes / elapsed;
                *speed_bps = if *speed_bps <= 0.0 {
                    instant_speed
                } else {
                    0.3 * instant_speed + 0.7 * *speed_bps
                };
                *speed_sample_at = std::time::Instant::now();
                *speed_sample_bytes = *done_bytes;
            }
        }
    }

    fn set_status(&mut self, msg: impl Into<String>, is_error: bool) {
        self.status_message = Some((msg.into(), is_error));
    }

    fn log_line(&mut self, line: String) {
        self.log.push_back(line);
        while self.log.len() > 300 {
            self.log.pop_front();
        }
    }

    fn client(&self) -> anyhow::Result<DriveClient> {
        let key = self
            .config
            .api_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Chưa cấu hình Google API key"))?;
        Ok(DriveClient::new(self.http.clone(), key))
    }

    /// Xử lý mọi sự kiện đã có sẵn trong hàng đợi (không chặn — nếu chưa có
    /// gì thì trả về ngay), gọi 1 lần ở đầu mỗi khung hình.
    fn drain_events(&mut self) {
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                WorkerEvent::BrowseResult {
                    folder_id,
                    folder_name,
                    entries,
                } => {
                    self.job = JobState::Idle;
                    self.current_entries = entries;
                    self.selected.clear();
                    self.selection_anchor = None;
                    self.renaming = None;
                    self.confirm_delete = None;
                    self.bulk_list_matches.clear();
                    self.bulk_list_unmatched.clear();
                    if self.breadcrumbs.is_empty() {
                        self.breadcrumbs.push((folder_id, folder_name));
                    } else if let Some(last) = self.breadcrumbs.last_mut() {
                        last.1 = folder_name;
                    }
                }
                WorkerEvent::BrowseError(e) => {
                    self.job = JobState::Idle;
                    self.set_status(e, true);
                }
                WorkerEvent::DestinationPicked(Some(path)) => {
                    self.destination = Some(path.clone());
                    self.config.last_destination = Some(path.clone());
                    let _ = self.config.save();
                    // Nếu người dùng vừa bấm "Tải" lúc chưa có thư mục đích,
                    // hộp thoại chọn thư mục được mở tự động (xem
                    // `start_download`) — giờ đã chọn xong thì tải tiếp luôn,
                    // không bắt bấm "Tải" lại lần nữa.
                    if let Some(entries) = self.pending_download.take() {
                        self.start_download_to(entries, path);
                    }
                }
                WorkerEvent::DestinationPicked(None) => {
                    // Người dùng bấm hủy hộp thoại chọn thư mục.
                    self.pending_download = None;
                }
                WorkerEvent::ScanProgress { found } => {
                    self.job = JobState::Scanning { found };
                }
                WorkerEvent::ScanError(e) => {
                    self.job = JobState::Idle;
                    self.set_status(e, true);
                }
                WorkerEvent::JobStarted {
                    total_files,
                    total_bytes,
                } => {
                    self.job = JobState::Downloading {
                        total_files,
                        total_bytes,
                        done_files: 0,
                        done_bytes: 0,
                        last_started_name: None,
                        speed_bps: 0.0,
                        speed_sample_at: std::time::Instant::now(),
                        speed_sample_bytes: 0,
                    };
                }
                WorkerEvent::FileStarted { name } => {
                    if let JobState::Downloading {
                        last_started_name, ..
                    } = &mut self.job
                    {
                        *last_started_name = Some(name);
                    }
                }
                WorkerEvent::FileRetrying {
                    name,
                    attempt,
                    max_attempts,
                    error,
                } => {
                    self.log_line(format!(
                        "↻ {name} — lần {attempt}/{max_attempts} lỗi: {error}"
                    ));
                }
                WorkerEvent::FileProgress { delta_bytes } => {
                    if let JobState::Downloading { done_bytes, .. } = &mut self.job {
                        *done_bytes += delta_bytes;
                    }
                }
                WorkerEvent::FileDone { name } => {
                    if let JobState::Downloading { done_files, .. } = &mut self.job {
                        *done_files += 1;
                    }
                    self.log_line(format!("✔ {name}"));
                }
                WorkerEvent::FileSkipped { name } => {
                    if let JobState::Downloading { done_files, .. } = &mut self.job {
                        *done_files += 1;
                    }
                    self.log_line(format!("⏭ {name} (đã có sẵn, bỏ qua)"));
                }
                WorkerEvent::FileFailed { name, error } => {
                    if let JobState::Downloading { done_files, .. } = &mut self.job {
                        *done_files += 1;
                    }
                    self.log_line(format!("✘ {name}: {error}"));
                }
                WorkerEvent::JobFinished {
                    succeeded,
                    failed,
                    cancelled,
                } => {
                    self.job = JobState::Idle;
                    if cancelled {
                        self.set_status(
                            format!("Đã hủy — đã tải xong {succeeded} file trước khi hủy."),
                            false,
                        );
                    } else if failed == 0 {
                        self.set_status(format!("Hoàn tất! Đã tải {succeeded} file."), false);
                    } else {
                        self.set_status(
                            format!(
                                "Hoàn tất với lỗi: {succeeded} thành công, {failed} thất bại (xem nhật ký bên dưới)."
                            ),
                            true,
                        );
                    }
                }
                WorkerEvent::FolderSizeResult {
                    folder_id,
                    files,
                    bytes,
                } => {
                    self.folder_sizes
                        .insert(folder_id, FolderSizeState::Known { files, bytes });
                }
                WorkerEvent::FolderSizeError { folder_id } => {
                    self.folder_sizes.insert(folder_id, FolderSizeState::Error);
                }
                WorkerEvent::LoginSucceeded { tokens, email } => {
                    self.login_busy = false;
                    self.config.oauth_tokens = Some(tokens);
                    self.config.oauth_email = email.clone();
                    let _ = self.config.save();
                    let who = email.unwrap_or_else(|| "(không rõ email)".to_string());
                    self.set_status(format!("Đã đăng nhập Google: {who}"), false);
                }
                WorkerEvent::LoginFailed(e) => {
                    self.login_busy = false;
                    self.set_status(format!("Đăng nhập thất bại: {e}"), true);
                }
                WorkerEvent::TokensRefreshed(tokens) => {
                    self.config.oauth_tokens = Some(tokens);
                    let _ = self.config.save();
                }
                WorkerEvent::FileRenamed { entry_id, new_name } => {
                    if let Some(entry) = self.current_entries.iter_mut().find(|e| e.id == entry_id) {
                        entry.name = new_name;
                    }
                    self.renaming = None;
                    self.set_status("Đã đổi tên.", false);
                }
                WorkerEvent::RenameFailed { error } => {
                    self.set_status(format!("Đổi tên thất bại: {error}"), true);
                }
                WorkerEvent::FileTrashed { entry_id, name } => {
                    self.current_entries.retain(|e| e.id != entry_id);
                    self.selected.remove(&entry_id);
                    self.log_line(format!("🗑 Đã chuyển vào Thùng rác: {name}"));
                }
                WorkerEvent::FileMovedAside { entry_id, name } => {
                    self.current_entries.retain(|e| e.id != entry_id);
                    self.selected.remove(&entry_id);
                    self.log_line(format!(
                        "↪ Đã chuyển '{name}' sang thư mục \"{QUARANTINE_FOLDER_NAME}\" \
                         (không phải chủ sở hữu nên không xóa thẳng được, chờ chủ sở hữu tự \
                         dọn)"
                    ));
                }
                WorkerEvent::TrashFailed { name, error } => {
                    self.log_line(format!("✘ Không xóa được {name}: {error}"));
                    self.set_status(format!("Không xóa được '{name}': {error}"), true);
                }
                WorkerEvent::OwnershipChecked { entry, info } => {
                    if info.owned_by_me {
                        // Là chủ sở hữu — trường hợp đơn giản/phổ biến nhất,
                        // xóa thẳng luôn, không hỏi thêm cho đỡ lằng nhằng.
                        self.confirm_delete = None;
                        self.start_delete(entry);
                    } else {
                        self.confirm_delete_move_aside = true;
                        // Ưu tiên parent_id Google trả về lúc kiểm tra sở
                        // hữu; chỉ dự phòng bằng thư mục đang mở khi Google
                        // không trả về gì (đã gặp thực tế với file bình
                        // thường) — KHÔNG ghi đè khi Google đã có trả lời.
                        let parent_id = info.parent_id.or_else(|| {
                            self.breadcrumbs.last().map(|(id, _)| id.clone())
                        });
                        self.confirm_delete = Some(ConfirmDeleteState::NotOwned {
                            entry,
                            owner_label: info.owner_label,
                            parent_id,
                        });
                    }
                }
                WorkerEvent::OwnershipCheckFailed { entry, error } => {
                    self.confirm_delete = None;
                    self.set_status(
                        format!("Không kiểm tra được quyền sở hữu '{}': {error}", entry.name),
                        true,
                    );
                }
                WorkerEvent::BulkTrashProgress { done, total } => {
                    self.job = JobState::BulkDeleting { done, total };
                }
                WorkerEvent::BulkTrashFinished {
                    succeeded,
                    failed,
                    skipped,
                } => {
                    self.job = JobState::Idle;
                    self.bulk_list_matches.clear();
                    self.bulk_list_unmatched.clear();
                    self.bulk_list_input.clear();
                    let skipped_note = if skipped > 0 {
                        format!(" ({skipped} bỏ qua vì không sở hữu)")
                    } else {
                        String::new()
                    };
                    if failed == 0 {
                        self.set_status(
                            format!("Đã xử lý xong {succeeded} file{skipped_note}."),
                            false,
                        );
                    } else {
                        self.set_status(
                            format!(
                                "Xong: {succeeded} thành công, {failed} thất bại{skipped_note} \
                                 (xem nhật ký)."
                            ),
                            true,
                        );
                    }
                }
            }
        }
    }

    fn fetch_folder(&mut self, folder_id: String, known_name: Option<String>) {
        let client = match self.client() {
            Ok(c) => c,
            Err(e) => {
                self.set_status(e.to_string(), true);
                self.job = JobState::Idle;
                return;
            }
        };
        self.job = JobState::Loading;
        let tx = self.event_tx.clone();
        self.runtime.spawn(async move {
            let name = match known_name {
                Some(n) => n,
                None => match client.get_metadata(&folder_id).await {
                    Ok(meta) => meta.name,
                    Err(e) => {
                        let _ = tx.send(WorkerEvent::BrowseError(e.to_string()));
                        return;
                    }
                },
            };
            match client.list_folder_children(&folder_id).await {
                Ok(entries) => {
                    let _ = tx.send(WorkerEvent::BrowseResult {
                        folder_id,
                        folder_name: name,
                        entries,
                    });
                }
                Err(e) => {
                    let _ = tx.send(WorkerEvent::BrowseError(e.to_string()));
                }
            }
        });
    }

    fn start_open_link(&mut self) {
        let id = match extract_drive_id(&self.link_input) {
            Ok(id) => id,
            Err(e) => {
                self.set_status(e.to_string(), true);
                return;
            }
        };
        self.breadcrumbs.clear();
        self.current_entries.clear();
        self.fetch_folder(id, None);
    }

    fn navigate_into(&mut self, id: String, name: String) {
        self.breadcrumbs.push((id.clone(), name.clone()));
        self.fetch_folder(id, Some(name));
    }

    fn navigate_to_breadcrumb(&mut self, index: usize) {
        if index + 1 >= self.breadcrumbs.len() {
            return;
        }
        self.breadcrumbs.truncate(index + 1);
        if let Some((id, name)) = self.breadcrumbs.last().cloned() {
            self.fetch_folder(id, Some(name));
        }
    }

    fn start_pick_folder(&mut self) {
        let tx = self.event_tx.clone();
        self.runtime.spawn(async move {
            let picked = rfd::AsyncFileDialog::new().pick_folder().await;
            let path = picked.map(|h| h.path().to_path_buf());
            let _ = tx.send(WorkerEvent::DestinationPicked(path));
        });
    }

    /// Quét đệ quy 1 thư mục CHỈ để lấy tổng số file + tổng dung lượng hiển
    /// thị cho người dùng xem — không tải gì cả. Chỉ chạy khi người dùng
    /// chủ động bấm xem (xem `draw_entry_list`), không tự động, vì 1 thư
    /// mục vài nghìn file có thể mất nhiều giây để quét xong.
    fn start_compute_folder_size(&mut self, folder_id: String) {
        if matches!(
            self.folder_sizes.get(&folder_id),
            Some(FolderSizeState::Computing)
        ) {
            return;
        }
        let client = match self.client() {
            Ok(c) => c,
            Err(e) => {
                self.set_status(e.to_string(), true);
                return;
            }
        };
        self.folder_sizes
            .insert(folder_id.clone(), FolderSizeState::Computing);
        let tx = self.event_tx.clone();
        self.runtime.spawn(async move {
            // Kênh "câm" — bước này không cần đẩy tiến độ "đang quét..." lên
            // UI, chỉ cần kết quả cuối cùng.
            let (silent_tx, _silent_rx) = tokio::sync::mpsc::unbounded_channel();
            let counter = AtomicUsize::new(0);
            let result = downloader::scan_folder_recursive(
                &client,
                &folder_id,
                std::path::Path::new(""),
                &counter,
                &silent_tx,
            )
            .await;
            match result {
                Ok(items) => {
                    let files = items.len();
                    let bytes = items.iter().filter_map(|i| i.entry.size).sum();
                    let _ = tx.send(WorkerEvent::FolderSizeResult {
                        folder_id,
                        files,
                        bytes,
                    });
                }
                Err(_) => {
                    let _ = tx.send(WorkerEvent::FolderSizeError { folder_id });
                }
            }
        });
    }

    fn start_download(&mut self, entries: Vec<DriveEntry>) {
        if entries.is_empty() {
            return;
        }
        let Some(dest) = self.destination.clone() else {
            // Chưa chọn thư mục lưu: nhớ lại danh sách đang muốn tải rồi mở
            // hộp thoại chọn thư mục ngay — chọn xong sẽ tự động tải tiếp
            // (xem nhánh `WorkerEvent::DestinationPicked` ở trên), người
            // dùng không cần bấm "Tải" lần thứ 2.
            self.pending_download = Some(entries);
            self.start_pick_folder();
            return;
        };
        self.start_download_to(entries, dest);
    }

    fn start_download_to(&mut self, entries: Vec<DriveEntry>, dest: PathBuf) {
        let client = match self.client() {
            Ok(c) => Arc::new(c),
            Err(e) => {
                self.set_status(e.to_string(), true);
                return;
            }
        };
        self.cancel_flag.store(false, Ordering::Relaxed);
        self.job = JobState::Scanning { found: 0 };
        self.status_message = None;
        let tx = self.event_tx.clone();
        let concurrency = self.config.max_concurrent_downloads.clamp(1, 16);
        let conflict_policy = self.config.conflict_policy;
        let cancel = self.cancel_flag.clone();

        self.runtime.spawn(async move {
            let items = match downloader::scan_all(&client, entries, &tx).await {
                Ok(items) => items,
                Err(e) => {
                    let _ = tx.send(WorkerEvent::ScanError(e.to_string()));
                    return;
                }
            };
            if items.is_empty() {
                let _ = tx.send(WorkerEvent::JobFinished {
                    succeeded: 0,
                    failed: 0,
                    cancelled: false,
                });
                return;
            }
            downloader::download_many(client, items, dest, concurrency, conflict_policy, cancel, tx)
                .await;
        });
    }

    fn is_logged_in(&self) -> bool {
        self.config.oauth_tokens.is_some()
    }

    fn start_login(&mut self) {
        let client_id = self.oauth_client_id_input.trim().to_string();
        let client_secret = self.oauth_client_secret_input.trim().to_string();
        if client_id.is_empty() || client_secret.is_empty() {
            self.set_status("Vui lòng nhập đủ OAuth Client ID và Client Secret.", true);
            return;
        }
        self.config.oauth_client_id = Some(client_id.clone());
        self.config.oauth_client_secret = Some(client_secret.clone());
        let _ = self.config.save();

        self.login_busy = true;
        self.status_message = None;
        let tx = self.event_tx.clone();
        let http = self.http.clone();
        self.runtime.spawn(async move {
            match crate::oauth::login(&http, client_id, client_secret).await {
                Ok(tokens) => {
                    let email = crate::oauth::fetch_user_email(&http, &tokens.access_token).await;
                    let _ = tx.send(WorkerEvent::LoginSucceeded { tokens, email });
                }
                Err(e) => {
                    let _ = tx.send(WorkerEvent::LoginFailed(e.to_string()));
                }
            }
        });
    }

    fn start_logout(&mut self) {
        self.config.oauth_tokens = None;
        self.config.oauth_email = None;
        let _ = self.config.save();
        self.set_status("Đã đăng xuất khỏi Google.", false);
    }

    /// Lấy sẵn (client_id, client_secret, tokens) đã cấu hình, dùng chung
    /// cho mọi thao tác ghi (đổi tên/xóa) — trả lỗi rõ ràng ngay nếu thiếu
    /// bước nào, thay vì để lỗi mơ hồ xảy ra sâu bên trong.
    fn oauth_credentials(&mut self) -> Option<(String, String, OAuthTokens)> {
        let Some(tokens) = self.config.oauth_tokens.clone() else {
            self.set_status("Chưa đăng nhập Google — vào mục Cài đặt để đăng nhập.", true);
            return None;
        };
        let Some(client_id) = self.config.oauth_client_id.clone() else {
            self.set_status("Thiếu OAuth Client ID.", true);
            return None;
        };
        let Some(client_secret) = self.config.oauth_client_secret.clone() else {
            self.set_status("Thiếu OAuth Client Secret.", true);
            return None;
        };
        Some((client_id, client_secret, tokens))
    }

    fn start_rename(&mut self, entry_id: String, new_name: String) {
        let new_name = new_name.trim().to_string();
        if new_name.is_empty() {
            self.set_status("Tên file không được để trống.", true);
            return;
        }
        let Some((client_id, client_secret, tokens)) = self.oauth_credentials() else {
            return;
        };
        let tx = self.event_tx.clone();
        let http = self.http.clone();
        self.runtime.spawn(async move {
            let access_token =
                match ensure_fresh_and_notify(&http, &client_id, &client_secret, tokens, &tx).await {
                    Ok(t) => t,
                    Err(e) => {
                        let _ = tx.send(WorkerEvent::RenameFailed {
                            error: e.to_string(),
                        });
                        return;
                    }
                };
            let edit_client = DriveEditClient::new(http);
            match edit_client.rename_file(&access_token, &entry_id, &new_name).await {
                Ok(()) => {
                    let _ = tx.send(WorkerEvent::FileRenamed { entry_id, new_name });
                }
                Err(e) => {
                    let _ = tx.send(WorkerEvent::RenameFailed {
                        error: e.to_string(),
                    });
                }
            }
        });
    }

    /// Kiểm tra tài khoản đang đăng nhập có phải chủ sở hữu file này không,
    /// TRƯỚC KHI hiện banner xác nhận xóa — vì Google chỉ cho phép CHỦ SỞ
    /// HỮU chuyển thẳng vào Thùng rác (quyền Editor qua link không đủ), nên
    /// cần biết trước để hỏi đúng câu hỏi (xóa thẳng, hay đề xuất chuyển
    /// sang thư mục riêng) thay vì để người dùng gặp lỗi 403 khó hiểu.
    fn start_check_ownership(&mut self, entry: DriveEntry) {
        let Some((client_id, client_secret, tokens)) = self.oauth_credentials() else {
            return;
        };
        self.confirm_delete = Some(ConfirmDeleteState::CheckingOwnership(entry.clone()));
        let tx = self.event_tx.clone();
        let http = self.http.clone();
        self.runtime.spawn(async move {
            let access_token =
                match ensure_fresh_and_notify(&http, &client_id, &client_secret, tokens, &tx).await {
                    Ok(t) => t,
                    Err(e) => {
                        let _ = tx.send(WorkerEvent::OwnershipCheckFailed {
                            entry,
                            error: e.to_string(),
                        });
                        return;
                    }
                };
            let edit_client = DriveEditClient::new(http);
            match edit_client.check_ownership(&access_token, &entry.id).await {
                Ok(info) => {
                    let _ = tx.send(WorkerEvent::OwnershipChecked { entry, info });
                }
                Err(e) => {
                    let _ = tx.send(WorkerEvent::OwnershipCheckFailed {
                        entry,
                        error: e.to_string(),
                    });
                }
            }
        });
    }

    /// Chuyển 1 file/thư mục vào Thùng rác. Gọi hàm này TRỰC TIẾP khi đã có
    /// xác nhận từ người dùng (xem `confirm_delete` trong `draw_job_status`
    /// hoặc `draw_entry_list`), bản thân hàm này KHÔNG hỏi lại nữa.
    fn start_delete(&mut self, entry: DriveEntry) {
        let Some((client_id, client_secret, tokens)) = self.oauth_credentials() else {
            return;
        };
        let tx = self.event_tx.clone();
        let http = self.http.clone();
        let entry_id = entry.id;
        let name = entry.name;
        self.runtime.spawn(async move {
            let access_token =
                match ensure_fresh_and_notify(&http, &client_id, &client_secret, tokens, &tx).await {
                    Ok(t) => t,
                    Err(e) => {
                        let _ = tx.send(WorkerEvent::TrashFailed {
                            name,
                            error: e.to_string(),
                        });
                        return;
                    }
                };
            let edit_client = DriveEditClient::new(http);
            match edit_client.trash_file(&access_token, &entry_id).await {
                Ok(()) => {
                    let _ = tx.send(WorkerEvent::FileTrashed { entry_id, name });
                }
                Err(e) => {
                    let _ = tx.send(WorkerEvent::TrashFailed {
                        name,
                        error: e.to_string(),
                    });
                }
            }
        });
    }

    /// Chuyển 1 file KHÔNG do tài khoản đang đăng nhập sở hữu sang thư mục
    /// riêng `QUARANTINE_FOLDER_NAME` (tạo ngay bên trong `parent_id` nếu
    /// chưa có) — thay thế cho việc xóa thẳng, vì Google chắc chắn từ chối
    /// (chỉ chủ sở hữu mới chuyển vào Thùng rác được).
    fn start_move_aside(&mut self, entry: DriveEntry, parent_id: String) {
        let Some((client_id, client_secret, tokens)) = self.oauth_credentials() else {
            return;
        };
        let tx = self.event_tx.clone();
        let http = self.http.clone();
        let entry_id = entry.id;
        let name = entry.name;
        self.runtime.spawn(async move {
            let access_token =
                match ensure_fresh_and_notify(&http, &client_id, &client_secret, tokens, &tx).await {
                    Ok(t) => t,
                    Err(e) => {
                        let _ = tx.send(WorkerEvent::TrashFailed {
                            name,
                            error: e.to_string(),
                        });
                        return;
                    }
                };
            let edit_client = DriveEditClient::new(http);
            let quarantine_id = match edit_client
                .find_or_create_folder(&access_token, &parent_id, QUARANTINE_FOLDER_NAME)
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    let _ = tx.send(WorkerEvent::TrashFailed {
                        name,
                        error: e.to_string(),
                    });
                    return;
                }
            };
            match edit_client
                .move_file(&access_token, &entry_id, &parent_id, &quarantine_id, None)
                .await
            {
                Ok(()) => {
                    let _ = tx.send(WorkerEvent::FileMovedAside { entry_id, name });
                }
                Err(e) => {
                    let _ = tx.send(WorkerEvent::TrashFailed {
                        name,
                        error: e.to_string(),
                    });
                }
            }
        });
    }

    /// So khớp danh sách đã dán/nhập với tên các mục đang hiển thị ở thư
    /// mục hiện tại (quy tắc khớp xem ở `match_names_in_folder`), rồi lưu kết
    /// quả để hiện xem trước — dùng chung cho cả TẢI lẫn xóa hàng loạt.
    fn find_bulk_list_matches(&mut self) {
        let segments = split_name_list(&self.bulk_list_input);
        if segments.is_empty() {
            self.bulk_list_matches.clear();
            self.bulk_list_unmatched.clear();
            self.set_status(
                "Danh sách đang trống — hãy dán tên file vào ô nhập trước.",
                true,
            );
            return;
        }

        let (matches, unmatched) = match_names_in_folder(&self.current_entries, &segments);
        self.bulk_list_matches = matches;
        self.bulk_list_unmatched = unmatched;
        if self.bulk_list_matches.is_empty() {
            self.set_status(
                "Không tìm thấy file nào khớp tên trong thư mục đang xem.",
                true,
            );
        } else {
            self.status_message = None;
        }
    }

    /// Kiểm tra quyền sở hữu VÀ xóa/chuyển đi LUÔN cho từng file — gộp 2
    /// giai đoạn "kiểm tra hết 784 file rồi mới hỏi rồi mới làm" thành 1
    /// giai đoạn liên tục, đỡ phải chờ xong hết mới bắt đầu làm file đầu
    /// tiên, và không cần hỏi xác nhận giữa chừng: rủi ro thực tế rất thấp
    /// vì file luôn còn nguyên trong Thùng rác (khôi phục được 30 ngày)
    /// hoặc trong thư mục tạm (`QUARANTINE_FOLDER_NAME`), không mất gì.
    /// `allow_move_aside`: người dùng đã đồng ý TRƯỚC (tick sẵn ở ô nhập,
    /// mặc định bật) cho chuyển các file KHÔNG do mình sở hữu vào thư mục
    /// tạm hay chưa — file do MÌNH sở hữu luôn được xóa thẳng, không phụ
    /// thuộc cờ này; file KHÔNG sở hữu mà cờ này tắt thì bị bỏ qua hoàn
    /// toàn (không đụng tới, tính vào `skipped`).
    fn start_bulk_delete(&mut self, entries: Vec<DriveEntry>, allow_move_aside: bool) {
        if entries.is_empty() {
            return;
        }
        let Some((client_id, client_secret, tokens)) = self.oauth_credentials() else {
            return;
        };
        self.job = JobState::BulkDeleting {
            done: 0,
            total: entries.len(),
        };
        self.status_message = None;
        let tx = self.event_tx.clone();
        let http = self.http.clone();
        // Giới hạn số request chạy đồng thời — dùng lại đúng số luồng tải
        // song song đã cấu hình, để không bắn quá nhiều request cùng lúc
        // lên Google (đỡ làm nặng thêm rủi ro rate-limit 429 đã biết).
        let concurrency = self.config.max_concurrent_downloads.max(1);
        // Thư mục đang mở — CHỈ dùng làm phương án dự phòng cho parent_id
        // khi Google không trả về field 'parents' lúc kiểm tra sở hữu (đã
        // gặp thực tế với file hoàn toàn bình thường). ƯU TIÊN giá trị
        // Google trả về khi CÓ.
        let fallback_parent_id = self.breadcrumbs.last().map(|(id, _)| id.clone());
        // Cache thư mục tạm theo parent_id, DÙNG CHUNG giữa mọi file chạy
        // song song — không có cache này, nhiều file cùng thư mục cha sẽ
        // ĐUA NHAU gọi find_or_create_folder gần như cùng lúc, mỗi request
        // đều thấy "chưa có" (vì chưa request nào kịp tạo xong) nên tự tạo
        // riêng, ra NHIỀU thư mục trùng tên (đã gặp thực tế). Giữ khóa
        // (Mutex) trong lúc tìm-hoặc-tạo: file nào tới trước làm luôn, các
        // file khác cùng thư mục cha xếp hàng chờ rồi dùng lại kết quả đã
        // có, không tự tạo thêm.
        let quarantine_cache: Arc<tokio::sync::Mutex<HashMap<String, String>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        self.runtime.spawn(async move {
            // Kiểm tra/làm mới ngay từ đầu — nếu ngay bước ĐẦU TIÊN này đã
            // lỗi (VD sai Client ID/Secret, hoặc refresh token đã bị Google
            // thu hồi) thì dừng NGAY, báo 1 lỗi rõ ràng duy nhất, thay vì
            // để tất cả N file đều tự thử rồi lỗi trùng lặp y hệt nhau.
            let initial_tokens = match crate::oauth::ensure_fresh(
                &http,
                &client_id,
                &client_secret,
                tokens.clone(),
            )
            .await
            {
                Ok(fresh) => {
                    if fresh.access_token != tokens.access_token {
                        let _ = tx.send(WorkerEvent::TokensRefreshed(fresh.clone()));
                    }
                    fresh
                }
                Err(e) => {
                    let _ = tx.send(WorkerEvent::TrashFailed {
                        name: "(không lấy được quyền truy cập)".to_string(),
                        error: e.to_string(),
                    });
                    let _ = tx.send(WorkerEvent::BulkTrashFinished {
                        succeeded: 0,
                        failed: entries.len(),
                        skipped: 0,
                    });
                    return;
                }
            };
            // Access token của Google chỉ sống khoảng 1 giờ. Lô xóa nhiều
            // file (nhất là khi gặp rate-limit phải lùi/thử lại) có thể
            // chạy LÂU HƠN thế — nếu chỉ lấy token 1 lần rồi dùng suốt,
            // phiên chạy dài dễ hết hạn giữa chừng, gây lỗi 401 hàng loạt
            // cho toàn bộ file còn lại (đã gặp thực tế: lô 533 file, quá
            // nửa số file cuối đều lỗi "invalid authentication
            // credentials"). Bọc token trong Mutex dùng chung: mỗi file tự
            // kiểm tra/làm mới NGAY TRƯỚC khi gọi API — is_expired() chỉ so
            // mốc thời gian đã lưu (rẻ, không gọi mạng) nên không tốn gì
            // thêm khi token còn hạn (trường hợp bình thường), chỉ thật sự
            // gọi Google làm mới khi cần.
            let shared_tokens: Arc<tokio::sync::Mutex<OAuthTokens>> =
                Arc::new(tokio::sync::Mutex::new(initial_tokens));
            let edit_client = DriveEditClient::new(http.clone());
            let total = entries.len();

            // Mỗi file: kiểm tra sở hữu rồi XỬ LÝ NGAY (không tách thành 2
            // lượt riêng như trước) — chạy song song có giới hạn, báo tiến
            // độ/kết quả ngay khi từng file xong (thứ tự dòng log có thể
            // xen kẽ khác thứ tự trong danh sách gốc 1 chút, số liệu tổng
            // cuối cùng không đổi).
            let mut result_stream = stream::iter(entries)
                .map(|entry| {
                    let edit_client = &edit_client;
                    let http = &http;
                    let client_id = &client_id;
                    let client_secret = &client_secret;
                    let shared_tokens = shared_tokens.clone();
                    let tx = tx.clone();
                    let fallback_parent_id = fallback_parent_id.clone();
                    let quarantine_cache = quarantine_cache.clone();
                    async move {
                        let entry_id = entry.id.clone();
                        let name = entry.name.clone();
                        // Giữ khóa SUỐT LÚC kiểm tra/làm mới — file khác
                        // đang chờ khóa này sẽ thấy NGAY token vừa làm mới
                        // khi tới lượt, không tự gọi Google làm mới thêm
                        // lần nữa (giống hệt cách cache thư mục tạm ở dưới).
                        let access_token = {
                            let mut guard = shared_tokens.lock().await;
                            let current = guard.clone();
                            match crate::oauth::ensure_fresh(
                                http,
                                client_id,
                                client_secret,
                                current.clone(),
                            )
                            .await
                            {
                                Ok(fresh) => {
                                    if fresh.access_token != current.access_token {
                                        *guard = fresh.clone();
                                        let _ =
                                            tx.send(WorkerEvent::TokensRefreshed(fresh.clone()));
                                    }
                                    fresh.access_token
                                }
                                Err(e) => {
                                    return (
                                        name,
                                        Err(format!("Không làm mới được phiên đăng nhập: {e}")),
                                    );
                                }
                            }
                        };
                        let access_token = access_token.as_str();
                        let outcome: Result<Option<WorkerEvent>, String> =
                            match edit_client.check_ownership(access_token, &entry_id).await {
                                Ok(info) if info.owned_by_me => edit_client
                                    .trash_file(access_token, &entry_id)
                                    .await
                                    .map(|()| {
                                        Some(WorkerEvent::FileTrashed {
                                            entry_id: entry_id.clone(),
                                            name: name.clone(),
                                        })
                                    })
                                    .map_err(|e| e.to_string()),
                                // Không sở hữu mà chưa được đồng ý chuyển
                                // đi — bỏ qua hoàn toàn, không đụng tới.
                                Ok(_) if !allow_move_aside => Ok(None),
                                Ok(info) => {
                                    match info.parent_id.or(fallback_parent_id) {
                                        Some(parent_id) => {
                                            // Giữ khóa SUỐT LÚC tìm-hoặc-tạo
                                            // (kể cả khi phải gọi API) — file
                                            // khác cùng parent_id đang chờ
                                            // khóa này sẽ thấy NGAY kết quả
                                            // vừa cache khi tới lượt, không
                                            // tự gọi API tạo thêm.
                                            let quarantine_id = {
                                                let mut cache = quarantine_cache.lock().await;
                                                if let Some(id) = cache.get(&parent_id) {
                                                    Ok(id.clone())
                                                } else {
                                                    let created = edit_client
                                                        .find_or_create_folder(
                                                            access_token,
                                                            &parent_id,
                                                            QUARANTINE_FOLDER_NAME,
                                                        )
                                                        .await;
                                                    if let Ok(id) = &created {
                                                        cache.insert(parent_id.clone(), id.clone());
                                                    }
                                                    created.map_err(|e| e.to_string())
                                                }
                                            };
                                            match quarantine_id {
                                                Ok(quarantine_id) => edit_client
                                                    .move_file(
                                                        access_token,
                                                        &entry_id,
                                                        &parent_id,
                                                        &quarantine_id,
                                                        None,
                                                    )
                                                    .await
                                                    .map(|()| {
                                                        Some(WorkerEvent::FileMovedAside {
                                                            entry_id: entry_id.clone(),
                                                            name: name.clone(),
                                                        })
                                                    })
                                                    .map_err(|e| e.to_string()),
                                                Err(e) => Err(e),
                                            }
                                        }
                                        None => Err(
                                            "Không xác định được thư mục cha để chuyển file đi"
                                                .to_string(),
                                        ),
                                    }
                                }
                                Err(e) => Err(format!("Không kiểm tra được quyền sở hữu: {e}")),
                            };
                        (name, outcome)
                    }
                })
                .buffer_unordered(concurrency);

            let mut succeeded = 0usize;
            let mut failed = 0usize;
            let mut skipped = 0usize;
            let mut done = 0usize;
            while let Some((name, outcome)) = result_stream.next().await {
                match outcome {
                    Ok(Some(event)) => {
                        succeeded += 1;
                        let _ = tx.send(event);
                    }
                    Ok(None) => {
                        skipped += 1;
                    }
                    Err(error) => {
                        failed += 1;
                        let _ = tx.send(WorkerEvent::TrashFailed {
                            name,
                            error,
                        });
                    }
                }
                done += 1;
                let _ = tx.send(WorkerEvent::BulkTrashProgress { done, total });
            }
            let _ = tx.send(WorkerEvent::BulkTrashFinished {
                succeeded,
                failed,
                skipped,
            });
        });
    }
}


// ============================================================================
// Giao diện
// ============================================================================

impl GDriveCopierApp {
    // ------------------------------------------------------------------
    // Tiện ích điều hướng cho thanh công cụ
    // ------------------------------------------------------------------

    /// Lên thư mục cha — chính là mục ngay trước trong breadcrumb.
    fn go_up(&mut self) {
        if self.breadcrumbs.len() >= 2 {
            let parent_index = self.breadcrumbs.len() - 2;
            self.navigate_to_breadcrumb(parent_index);
        }
    }

    /// Tải lại danh sách của thư mục đang xem (vd. sau khi chủ sở hữu vừa
    /// thêm/xóa file ở phía Drive).
    fn reload_current(&mut self) {
        if let Some((id, name)) = self.breadcrumbs.last().cloned() {
            self.fetch_folder(id, Some(name));
        }
    }

    /// Mở thư mục đích bằng trình quản lý file của hệ điều hành. Chạy trên
    /// luồng nền để giao diện không bị khựng nếu hệ điều hành phản hồi chậm.
    fn open_destination(&self) {
        if let Some(path) = self.destination.clone() {
            self.runtime.spawn_blocking(move || {
                let _ = open::that(path);
            });
        }
    }

    /// Kéo-thả 1 file .txt chứa danh sách vào cửa sổ app -> tự đọc nội
    /// dung, NỐI THÊM vào ô nhập của "Tải / xóa hàng loạt" (không xóa nội
    /// dung đã gõ sẵn, để không mất công nếu người dùng đã tự nhập một phần
    /// trước đó) rồi chuyển sang tab đó cho người dùng thấy ngay. Gọi 1 lần
    /// mỗi khung hình, hoạt động bất kể tab nào đang mở hay đã đăng nhập
    /// chưa (tải theo danh sách không cần đăng nhập).
    fn handle_dropped_files(&mut self, ctx: &egui::Context) {
        let dropped_files = ctx.input(|i| i.raw.dropped_files.clone());
        for file in &dropped_files {
            let Some(path) = &file.path else { continue };
            let is_txt = path
                .extension()
                .map(|e| e.eq_ignore_ascii_case("txt"))
                .unwrap_or(false);
            if !is_txt {
                continue;
            }
            match std::fs::read_to_string(path) {
                Ok(content) => {
                    // Notepad (Windows) hay lưu .txt kiểu "UTF-8 with BOM":
                    // ký tự BOM (U+FEFF) không phải khoảng trắng nên `trim()`
                    // không bỏ được, sẽ dính vào tên đầu tiên làm nó không
                    // bao giờ khớp — phải bỏ riêng.
                    let content = content.trim_start_matches('\u{feff}').trim_end();
                    if !self.bulk_list_input.trim().is_empty() {
                        self.bulk_list_input.push('\n');
                    }
                    self.bulk_list_input.push_str(content);
                    self.activity_tab = ActivityTab::BulkList;
                    self.set_status(format!("Đã nạp danh sách từ file: {}", path.display()), false);
                }
                Err(e) => {
                    self.set_status(format!("Không đọc được file {}: {e}", path.display()), true);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Thanh trên: link Drive (trái) + tài khoản & cài đặt (phải)
    // ------------------------------------------------------------------

    fn draw_top_bar(&mut self, ui: &mut egui::Ui) {
        let ready = self.config.api_key.is_some();
        ui.allocate_ui_with_layout(
            vec2(ui.available_width(), 30.0),
            Layout::right_to_left(Align::Center),
            |ui| {
                // Nhóm bên phải thêm TRƯỚC (right-to-left) để nằm sát mép phải;
                // phần chỗ trống còn lại bên trái dành hết cho ô nhập link.
                let gear = egui::Button::new("⚙").selected(self.show_settings);
                if ui.add(gear).on_hover_text("Cài đặt").clicked() {
                    self.show_settings = !self.show_settings;
                }
                self.draw_account_status(ui);
                if ready {
                    ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
                        self.draw_link_input(ui);
                    });
                }
            },
        );
    }

    fn draw_link_input(&mut self, ui: &mut egui::Ui) {
        let busy = self.is_busy();
        let open_w = 64.0;
        let input_w =
            (ui.available_width() - open_w - ui.spacing().item_spacing.x).clamp(140.0, 900.0);
        let resp = ui.add_enabled(
            !busy,
            egui::TextEdit::singleline(&mut self.link_input)
                .desired_width(input_w)
                .margin(egui::Margin::symmetric(8, 5))
                .hint_text("Dán link thư mục Google Drive công khai..."),
        );
        let enter_pressed = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
        let open_clicked = ui
            .add_enabled(!busy, egui::Button::new("Mở").min_size(vec2(open_w, 26.0)))
            .clicked();
        if enter_pressed || open_clicked {
            self.show_settings = false;
            self.start_open_link();
        }
    }

    /// Trạng thái đăng nhập Google, hiện Ở GÓC PHẢI thanh trên — LUÔN thấy
    /// được (không cần mở Cài đặt) để biết ngay đang thao tác bằng tài
    /// khoản nào, quan trọng vì việc xóa phụ thuộc đúng tài khoản đang đăng
    /// nhập (xem `ConfirmDeleteState`). Gọi trong layout right-to-left.
    fn draw_account_status(&mut self, ui: &mut egui::Ui) {
        if self.is_logged_in() {
            if ui.small_button("Đăng xuất").clicked() {
                self.start_logout();
            }
            let who = self
                .config
                .oauth_email
                .clone()
                .unwrap_or_else(|| "(không rõ email)".to_string());
            ui.colored_label(tone_color(ui, Tone::Ok), format!("✔ {who}"));
        } else if self.login_busy {
            ui.spinner();
            ui.weak("Đang chờ đăng nhập...");
        } else if self.config.oauth_client_id.is_some() && self.config.oauth_client_secret.is_some()
        {
            if ui.small_button("Đăng nhập Google").clicked() {
                self.start_login();
            }
        } else {
            if ui.small_button("Cài đặt...").clicked() {
                self.show_settings = true;
            }
            ui.weak("Chưa đăng nhập:");
        }
    }

    // ------------------------------------------------------------------
    // Trang Cài đặt: 2 thẻ cạnh nhau (API key | Đăng nhập Google)
    // ------------------------------------------------------------------

    fn draw_settings_page(&mut self, ui: &mut egui::Ui) {
        self.draw_notice(ui);
        egui::ScrollArea::vertical()
            .id_salt("settings_scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.heading("Cài đặt");
                ui.add_space(6.0);
                ui.columns(2, |cols| {
                    self.draw_api_key_card(&mut cols[0]);
                    self.draw_oauth_card(&mut cols[1]);
                });
            });
    }

    fn draw_api_key_card(&mut self, ui: &mut egui::Ui) {
        card(ui, |ui| {
            ui.strong("Google API key");
            ui.weak(
                "Cần có Google API key (miễn phí) để app đọc được dữ liệu Drive công khai \
                 mà không bắt bạn đăng nhập. Xem hướng dẫn lấy key trong README.md đi kèm.",
            );
            ui.add_space(6.0);
            ui.label("API key");
            ui.add(
                egui::TextEdit::singleline(&mut self.api_key_input)
                    .password(true)
                    .desired_width(f32::INFINITY),
            );
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                let can_save = !self.api_key_input.trim().is_empty();
                if ui
                    .add_enabled(can_save, egui::Button::new("Lưu"))
                    .clicked()
                {
                    self.config.api_key = Some(self.api_key_input.trim().to_string());
                    if let Err(e) = self.config.save() {
                        self.set_status(format!("Không lưu được cấu hình: {e}"), true);
                    } else {
                        self.show_settings = false;
                        self.set_status("Đã lưu API key.", false);
                    }
                }
                if self.config.api_key.is_some() && ui.button("Đóng").clicked() {
                    self.show_settings = false;
                }
            });
        });
    }

    fn draw_oauth_card(&mut self, ui: &mut egui::Ui) {
        card(ui, |ui| {
            ui.strong("Đổi tên / xóa file (không bắt buộc)");
            ui.weak(
                "Chỉ cần nếu muốn đổi tên hoặc chuyển file vào Thùng rác trên thư mục đã chia sẻ \
                 quyền chỉnh sửa. Không liên quan gì tới API key — cần đăng nhập Google 1 \
                 lần. Xem hướng dẫn tạo OAuth Client ID trong README.md.",
            );
            ui.add_space(6.0);
            ui.label("OAuth Client ID");
            ui.add(
                egui::TextEdit::singleline(&mut self.oauth_client_id_input)
                    .desired_width(f32::INFINITY),
            );
            ui.label("OAuth Client Secret");
            ui.add(
                egui::TextEdit::singleline(&mut self.oauth_client_secret_input)
                    .password(true)
                    .desired_width(f32::INFINITY),
            );
            ui.add_space(6.0);

            if self.is_logged_in() {
                let who = self
                    .config
                    .oauth_email
                    .clone()
                    .unwrap_or_else(|| "(không rõ email)".to_string());
                ui.horizontal(|ui| {
                    ui.colored_label(tone_color(ui, Tone::Ok), format!("✔ Đã đăng nhập: {who}"));
                    if ui.button("Đăng xuất").clicked() {
                        self.start_logout();
                    }
                });
            } else {
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(!self.login_busy, egui::Button::new("Đăng nhập Google"))
                        .clicked()
                    {
                        self.start_login();
                    }
                    if self.login_busy {
                        ui.spinner();
                        ui.weak("Đang chờ bạn đăng nhập trên trình duyệt...");
                    }
                });
            }
        });
    }

    // ------------------------------------------------------------------
    // Vùng chính (giữa): duyệt thư mục Drive
    // ------------------------------------------------------------------

    fn draw_browser(&mut self, ui: &mut egui::Ui) {
        self.draw_path_bar(ui);
        self.draw_notice(ui);
        self.draw_delete_confirm(ui);
        if self.current_entries.is_empty() {
            self.draw_empty_state(ui);
            return;
        }
        self.draw_list_toolbar(ui);
        self.draw_file_table(ui);
    }

    /// ⬆ lên thư mục cha, ⟳ tải lại, rồi breadcrumb (bấm vào để quay lại).
    fn draw_path_bar(&mut self, ui: &mut egui::Ui) {
        let busy = self.is_busy();
        let can_up = !busy && self.breadcrumbs.len() >= 2;
        let can_reload = !busy && !self.breadcrumbs.is_empty();
        let loading = matches!(self.job, JobState::Loading);
        let mut go_to: Option<usize> = None;
        let mut go_up = false;
        let mut reload = false;
        ui.horizontal_wrapped(|ui| {
            if ui
                .add_enabled(can_up, egui::Button::new("⬆"))
                .on_hover_text("Lên thư mục cha")
                .clicked()
            {
                go_up = true;
            }
            if ui
                .add_enabled(can_reload, egui::Button::new("⟳"))
                .on_hover_text("Tải lại thư mục này")
                .clicked()
            {
                reload = true;
            }
            if self.breadcrumbs.is_empty() {
                ui.weak("Chưa mở thư mục nào");
            }
            let last_idx = self.breadcrumbs.len().saturating_sub(1);
            for (i, (_, name)) in self.breadcrumbs.iter().enumerate() {
                if i > 0 {
                    ui.weak("›");
                }
                if i == last_idx {
                    ui.strong(name);
                } else if ui
                    .add_enabled(!busy, egui::Link::new(name.as_str()))
                    .clicked()
                {
                    go_to = Some(i);
                }
            }
            if loading {
                ui.spinner();
            }
        });
        if go_up {
            self.go_up();
        } else if reload {
            self.reload_current();
        } else if let Some(i) = go_to {
            self.navigate_to_breadcrumb(i);
        }
    }

    /// Thông báo trạng thái (thành công/lỗi) đặt ngay phía trên danh sách
    /// để không bị đẩy ra khỏi tầm nhìn, có nút ✕ để đóng.
    fn draw_notice(&mut self, ui: &mut egui::Ui) {
        let Some((msg, is_error)) = self.status_message.clone() else {
            return;
        };
        let color = tone_color(ui, if is_error { Tone::Error } else { Tone::Ok });
        let mut dismiss = false;
        banner_frame(color).show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            ui.with_layout(Layout::right_to_left(Align::Min), |ui| {
                if ui.small_button("✕").on_hover_text("Đóng").clicked() {
                    dismiss = true;
                }
                ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
                    ui.add(egui::Label::new(RichText::new(msg).color(color)).wrap());
                });
            });
        });
        if dismiss {
            self.status_message = None;
        }
    }

    /// Banner xác nhận xóa 1 file (xem `ConfirmDeleteState`): đang kiểm tra
    /// chủ sở hữu, hoặc hỏi có cho chuyển sang thư mục tạm không (khi
    /// KHÔNG phải chủ sở hữu). Chủ sở hữu thì xóa thẳng, không có banner.
    fn draw_delete_confirm(&mut self, ui: &mut egui::Ui) {
        let Some(state) = self.confirm_delete.clone() else {
            return;
        };
        match state {
            ConfirmDeleteState::CheckingOwnership(entry) => {
                let color = ui.visuals().weak_text_color();
                banner_frame(color).show(ui, |ui| {
                    ui.set_min_width(ui.available_width());
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label(format!("Đang kiểm tra quyền sở hữu '{}'...", entry.name));
                    });
                });
            }
            ConfirmDeleteState::NotOwned {
                entry,
                owner_label,
                parent_id,
            } => {
                let warn = tone_color(ui, Tone::Warn);
                banner_frame(warn).show(ui, |ui| {
                    ui.set_min_width(ui.available_width());
                    let owner = owner_label.as_deref().unwrap_or("không rõ");
                    ui.add(
                        egui::Label::new(
                            RichText::new(format!(
                                "'{}' không phải của bạn (chủ sở hữu: {owner}) nên Google không \
                                 cho xóa thẳng.",
                                entry.name
                            ))
                            .color(warn),
                        )
                        .wrap(),
                    );
                    ui.horizontal_wrapped(|ui| {
                        ui.checkbox(
                            &mut self.confirm_delete_move_aside,
                            "Cho phép chuyển vào thư mục tạm",
                        );
                        let can_proceed = self.confirm_delete_move_aside && parent_id.is_some();
                        if ui
                            .add_enabled(can_proceed, egui::Button::new("Xóa"))
                            .clicked()
                        {
                            self.confirm_delete = None;
                            if let Some(parent_id) = parent_id.clone() {
                                self.start_move_aside(entry.clone(), parent_id);
                            }
                        }
                        if ui.button("Hủy").clicked() {
                            self.confirm_delete = None;
                        }
                    });
                    if parent_id.is_none() {
                        ui.weak(
                            "(Không xác định được thư mục cha của file này nên chưa thể chuyển \
                             đi — thử lại từ đầu.)",
                        );
                    }
                });
            }
        }
    }

    fn draw_empty_state(&self, ui: &mut egui::Ui) {
        let loading = matches!(self.job, JobState::Loading);
        ui.vertical_centered(|ui| {
            ui.add_space((ui.available_height() * 0.28).max(12.0));
            if loading {
                ui.spinner();
                ui.label("Đang tải danh sách...");
            } else if self.breadcrumbs.is_empty() {
                ui.label(RichText::new("📂").size(40.0));
                ui.label("Dán link một thư mục Google Drive công khai lên thanh trên rồi bấm \"Mở\".");
            } else {
                ui.label(RichText::new("📂").size(40.0));
                ui.label("Thư mục này trống.");
            }
        });
    }

    /// Dòng công cụ phía trên bảng: số mục + tóm tắt phần đã chọn (trái),
    /// nút Tải (phải).
    fn draw_list_toolbar(&mut self, ui: &mut egui::Ui) {
        let busy = self.is_busy();
        let n = self.current_entries.len();
        let (sel_count, sel_bytes, sel_unknown) =
            selection_summary(&self.current_entries, &self.selected, &self.folder_sizes);
        let mut download_all = false;
        let mut download_selected = false;

        ui.allocate_ui_with_layout(
            vec2(ui.available_width(), 30.0),
            Layout::right_to_left(Align::Center),
            |ui| {
                let primary =
                    egui::Button::new(RichText::new("⬇ Tải đã chọn").color(Color32::WHITE))
                        .fill(ACCENT);
                if ui
                    .add_enabled(!busy && sel_count > 0, primary)
                    .clicked()
                {
                    download_selected = true;
                }
                if ui
                    .add_enabled(!busy, egui::Button::new("⬇ Tải tất cả"))
                    .on_hover_text("Tải tất cả mục đang hiển thị")
                    .clicked()
                {
                    download_all = true;
                }
                ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
                    let mut text = format!("{n} mục");
                    if sel_count > 0 {
                        text.push_str(&format!(
                            " · đã chọn {sel_count} (~{})",
                            format_bytes(sel_bytes)
                        ));
                        if sel_unknown > 0 {
                            text.push_str(&format!(
                                " · +{sel_unknown} thư mục chưa rõ dung lượng"
                            ));
                        }
                    }
                    ui.add(egui::Label::new(text).truncate());
                });
            },
        );

        if download_all {
            let entries = self.current_entries.clone();
            self.start_download(entries);
        }
        if download_selected {
            let sel: Vec<DriveEntry> = self
                .current_entries
                .iter()
                .filter(|e| self.selected.contains(&e.id))
                .cloned()
                .collect();
            self.selected.clear();
            self.start_download(sel);
        }
    }

    /// Bảng file: tiêu đề cột + danh sách cuộn (chỉ vẽ các dòng đang nhìn
    /// thấy). Mọi thao tác của người dùng được gom vào `TableActions` rồi
    /// mới áp lên `self` SAU KHI vẽ xong (tránh vay mượn `self` trong
    /// closure của `show_rows`).
    fn draw_file_table(&mut self, ui: &mut egui::Ui) {
        let env_can_download = !self.is_busy();
        let env_logged_in = self.is_logged_in();
        let n = self.current_entries.len();
        let selected_in_list = self
            .current_entries
            .iter()
            .filter(|e| self.selected.contains(&e.id))
            .count();
        let all_selected = n > 0 && selected_in_list == n;
        let partially_selected = selected_in_list > 0 && !all_selected;
        // Mỗi thư mục nhớ vị trí cuộn riêng (quay lại thư mục cha vẫn đứng
        // đúng chỗ cũ, còn vào thư mục mới thì bắt đầu từ đầu danh sách).
        let list_id = self
            .breadcrumbs
            .last()
            .map(|(id, _)| id.clone())
            .unwrap_or_default();
        let mut actions = TableActions::default();

        {
            let entries = &self.current_entries;
            let env = RowEnv {
                selected: &self.selected,
                folder_sizes: &self.folder_sizes,
                can_download: env_can_download,
                logged_in: env_logged_in,
            };
            let renaming = &mut self.renaming;
            let rename_focus = &mut self.rename_focus;

            ui.scope(|ui| {
                // Không chừa khoảng hở dọc giữa các dòng: `show_rows` tính
                // vị trí dòng theo chiều cao cố định + khoảng hở này, còn
                // các dòng tự tô nền kẻ sọc nên cần liền khít nhau.
                ui.spacing_mut().item_spacing.y = 0.0;
                draw_table_header(ui, all_selected, partially_selected, &mut actions);
                egui::ScrollArea::vertical()
                    .id_salt(("file_list", list_id))
                    .auto_shrink([false, false])
                    .show_rows(ui, ROW_H, n, |ui, row_range| {
                        for i in row_range {
                            draw_entry_row(
                                ui,
                                i,
                                &entries[i],
                                &env,
                                renaming,
                                rename_focus,
                                &mut actions,
                            );
                        }
                    });
            });
        }

        // --- Áp mọi thay đổi vào self SAU KHI vẽ xong.
        if let Some(select_all) = actions.toggle_select_all {
            if select_all {
                for e in &self.current_entries {
                    self.selected.insert(e.id.clone());
                }
            } else {
                self.selected.clear();
            }
        }
        if let Some((index, shift_held, checked)) = actions.selection_change {
            let len = self.current_entries.len();
            if index < len {
                if shift_held {
                    if let Some(anchor) = self.selection_anchor {
                        let (lo, hi) = (anchor.min(index), anchor.max(index).min(len - 1));
                        for e in &self.current_entries[lo..=hi] {
                            self.selected.insert(e.id.clone());
                        }
                    } else {
                        // Chưa có điểm neo (lần tick đầu tiên) -> xử lý như tick đơn.
                        set_selected(&mut self.selected, &self.current_entries[index].id, checked);
                        self.selection_anchor = Some(index);
                    }
                } else {
                    set_selected(&mut self.selected, &self.current_entries[index].id, checked);
                    self.selection_anchor = Some(index);
                }
            }
        }
        if let Some(pair) = actions.start_rename_for {
            self.renaming = Some(pair);
            self.rename_focus = true;
        }
        if actions.rename_cancelled {
            self.renaming = None;
        }
        if let Some((id, new_name)) = actions.rename_confirmed {
            self.start_rename(id, new_name);
        }
        if let Some(entry) = actions.delete_clicked {
            self.start_check_ownership(entry);
        }
        if let Some(entry) = actions.download_single {
            self.start_download(vec![entry]);
        }
        if let Some((id, name)) = actions.navigate_to {
            self.navigate_into(id, name);
        }
        if let Some(folder_id) = actions.compute_size_for {
            self.start_compute_folder_size(folder_id);
        }
    }

    // ------------------------------------------------------------------
    // Thanh dưới: nơi lưu + cách xử lý trùng tên
    // ------------------------------------------------------------------

    fn draw_bottom_bar(&mut self, ui: &mut egui::Ui) {
        let mut pick = false;
        let mut open_folder = false;
        ui.horizontal_wrapped(|ui| {
            ui.label("Lưu vào:");
            let full = self
                .destination
                .as_ref()
                .map(|p| p.display().to_string());
            let shown = full.clone().unwrap_or_else(|| "(chưa chọn)".to_string());
            let path_w = (ui.available_width() * 0.5).clamp(180.0, 480.0);
            ui.allocate_ui_with_layout(
                vec2(path_w, 24.0),
                Layout::left_to_right(Align::Center),
                |ui| {
                    let resp = ui.add(
                        egui::Label::new(RichText::new(shown).monospace())
                            .truncate()
                            .show_tooltip_when_elided(false),
                    );
                    if let Some(full) = &full {
                        resp.on_hover_text(full.as_str());
                    }
                },
            );
            if ui.button("Chọn thư mục...").clicked() {
                pick = true;
            }
            if self.destination.is_some()
                && ui
                    .button("Mở")
                    .on_hover_text("Mở thư mục này trong trình quản lý file")
                    .clicked()
            {
                open_folder = true;
            }

            ui.separator();

            ui.label("Nếu trùng tên:");
            // Dùng biến cục bộ cho ComboBox, không đụng `self` bên trong
            // closure lồng của `show_ui` — chỉ ghi lại vào self SAU KHI
            // combo box đã đóng, để chắc chắn không vướng borrow-checker.
            let mut new_policy = self.config.conflict_policy;
            egui::ComboBox::from_id_salt("conflict_policy_combo")
                .selected_text(new_policy.label())
                .show_ui(ui, |ui| {
                    for policy in [
                        ConflictPolicy::Skip,
                        ConflictPolicy::Overwrite,
                        ConflictPolicy::Rename,
                    ] {
                        ui.selectable_value(&mut new_policy, policy, policy.label());
                    }
                });
            if new_policy != self.config.conflict_policy {
                self.config.conflict_policy = new_policy;
                let _ = self.config.save();
            }
        });
        if pick {
            self.start_pick_folder();
        }
        if open_folder {
            self.open_destination();
        }
    }

    // ------------------------------------------------------------------
    // Khung bên phải: tiến độ + (nhật ký | tải/xóa hàng loạt)
    // ------------------------------------------------------------------

    fn draw_activity_panel(&mut self, ui: &mut egui::Ui) {
        self.draw_job_card(ui);
        ui.add_space(4.0);

        // Tải theo danh sách KHÔNG cần đăng nhập (dùng API key như mọi cách
        // tải khác), chỉ cần đã mở 1 thư mục để có mục mà khớp tên; riêng
        // phần xóa trong cùng tab mới cần đăng nhập (xem `draw_bulk_list_tab`).
        let bulk_available = self.is_logged_in() || !self.current_entries.is_empty();
        if !bulk_available && self.activity_tab == ActivityTab::BulkList {
            self.activity_tab = ActivityTab::Log;
        }
        ui.horizontal(|ui| {
            ui.selectable_value(
                &mut self.activity_tab,
                ActivityTab::Log,
                format!("Nhật ký ({})", self.log.len()),
            );
            if bulk_available {
                ui.selectable_value(
                    &mut self.activity_tab,
                    ActivityTab::BulkList,
                    "Tải / Xóa hàng loạt",
                );
            }
        });
        ui.separator();

        match self.activity_tab {
            ActivityTab::Log => self.draw_log_tab(ui),
            ActivityTab::BulkList => self.draw_bulk_list_tab(ui),
        }
    }

    /// Tiến độ của tác vụ đang chạy (luôn ở đầu khung bên phải, không bị
    /// đẩy đi mất dù đang duyệt/chọn ở vùng giữa).
    fn draw_job_card(&mut self, ui: &mut egui::Ui) {
        ui.strong("Tiến độ");
        let mut cancel = false;
        match &self.job {
            JobState::Idle => {
                ui.weak("Chưa có tác vụ nào đang chạy.");
            }
            JobState::Loading => {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("Đang tải danh sách...");
                });
            }
            JobState::Scanning { found } => {
                ui.horizontal_wrapped(|ui| {
                    ui.spinner();
                    ui.label(format!("Đang quét thư mục... đã tìm thấy {found} file"));
                });
                if ui.button("Hủy").clicked() {
                    cancel = true;
                }
            }
            JobState::Downloading {
                total_files,
                total_bytes,
                done_files,
                done_bytes,
                last_started_name,
                speed_bps,
                ..
            } => {
                let frac = if *total_files > 0 {
                    *done_files as f32 / *total_files as f32
                } else {
                    0.0
                };
                ui.add(
                    egui::ProgressBar::new(frac)
                        .text(format!("{done_files}/{total_files} file"))
                        .desired_height(20.0),
                );
                egui::Grid::new("job_stats")
                    .num_columns(2)
                    .spacing([12.0, 3.0])
                    .show(ui, |ui| {
                        ui.weak("Dung lượng");
                        ui.label(format!(
                            "{} / {}",
                            format_bytes(*done_bytes),
                            format_bytes(*total_bytes)
                        ));
                        ui.end_row();
                        ui.weak("Tốc độ");
                        ui.label(format_speed(*speed_bps));
                        ui.end_row();
                        let remaining_bytes = total_bytes.saturating_sub(*done_bytes);
                        if *speed_bps >= 1024.0 && remaining_bytes > 0 {
                            let eta_secs = remaining_bytes as f64 / *speed_bps;
                            ui.weak("Còn lại");
                            ui.label(format!("khoảng {}", format_duration(eta_secs)));
                            ui.end_row();
                        }
                    });
                if let Some(name) = last_started_name {
                    ui.add(
                        egui::Label::new(RichText::new(format!("Đang tải: {name}")).weak())
                            .truncate(),
                    );
                }
                if ui.button("Hủy").clicked() {
                    cancel = true;
                }
            }
            JobState::BulkDeleting { done, total } => {
                let frac = if *total > 0 {
                    *done as f32 / *total as f32
                } else {
                    0.0
                };
                ui.add(
                    egui::ProgressBar::new(frac)
                        .text(format!("Đang chuyển vào Thùng rác: {done}/{total}"))
                        .desired_height(20.0),
                );
            }
        }
        if cancel {
            self.cancel_flag.store(true, Ordering::Relaxed);
        }
    }

    fn draw_log_tab(&mut self, ui: &mut egui::Ui) {
        if self.log.is_empty() {
            ui.weak("Chưa có nhật ký.");
            return;
        }
        let mut clear = false;
        ui.horizontal(|ui| {
            ui.weak(format!("{} dòng", self.log.len()));
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if ui.small_button("Xóa nhật ký").clicked() {
                    clear = true;
                }
            });
        });
        egui::ScrollArea::vertical()
            .id_salt("log_scroll")
            .auto_shrink([false, false])
            .stick_to_bottom(true)
            .show(ui, |ui| {
                for line in &self.log {
                    ui.add(egui::Label::new(log_line_text(ui, line)).wrap());
                }
            });
        if clear {
            self.log.clear();
        }
    }

    /// Ô nhập danh sách tên + xem trước các mục khớp trong thư mục đang xem.
    /// Cùng 1 danh sách dùng được cho 2 việc (xem `draw_activity_panel` cho
    /// điều kiện hiện tab này): TẢI về máy — luôn có, KHÔNG cần đăng nhập
    /// (vẫn dùng API key như các cách tải khác); chuyển vào Thùng rác — chỉ
    /// hiện khi đã đăng nhập Google.
    fn draw_bulk_list_tab(&mut self, ui: &mut egui::Ui) {
        let busy = self.is_busy();
        let logged_in = self.is_logged_in();
        let mut find_clicked = false;
        let mut download_clicked = false;
        let mut delete_clicked = false;

        egui::ScrollArea::vertical()
            .id_salt("bulk_tab_scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.weak(
                    "Dán danh sách tên (mỗi tên 1 dòng, hoặc cách nhau bằng dấu phẩy/chấm \
                     phẩy/khoảng trắng), hoặc kéo-thả 1 file .txt vào cửa sổ. Tên phải khớp \
                     CHÍNH XÁC tên đang hiển thị trong thư mục đang xem. Tải thì không cần \
                     đăng nhập, xóa thì cần.",
                );
                ui.add_space(4.0);
                // Giới hạn chiều cao hiển thị — không bọc thì egui tự giãn
                // ô nhập cao theo đúng số dòng nội dung, dán danh sách vài
                // trăm dòng sẽ chiếm hết khung, khó theo dõi các phần bên
                // dưới. Nội dung dài hơn khung vẫn cuộn được bình thường,
                // không mất chữ.
                egui::ScrollArea::vertical()
                    .id_salt("bulk_list_input_scroll")
                    .max_height(120.0)
                    .show(ui, |ui| {
                        ui.add(
                            egui::TextEdit::multiline(&mut self.bulk_list_input)
                                .desired_rows(5)
                                .desired_width(f32::INFINITY)
                                .hint_text("vidu_1.mp4, vidu_2.jpg\nvidu_3.mp4; vidu_4.png\n..."),
                        );
                    });
                ui.add_space(4.0);
                if ui
                    .add_enabled(!busy, egui::Button::new("Tìm & xem trước"))
                    .clicked()
                {
                    find_clicked = true;
                }

                if !self.bulk_list_matches.is_empty() || !self.bulk_list_unmatched.is_empty() {
                    ui.add_space(8.0);
                    ui.label(format!(
                        "Khớp {} mục, {} tên không khớp mục nào trong thư mục đang xem",
                        self.bulk_list_matches.len(),
                        self.bulk_list_unmatched.len()
                    ));
                }
                if !self.bulk_list_matches.is_empty() {
                    let row_h = ui.text_style_height(&egui::TextStyle::Body);
                    let matches = &self.bulk_list_matches;
                    egui::ScrollArea::vertical()
                        .id_salt("bulk_list_matches_scroll")
                        .max_height(180.0)
                        .show_rows(ui, row_h, matches.len(), |ui, row_range| {
                            for i in row_range {
                                let e = &matches[i];
                                let mut line = format!("• {}  {}", entry_icon(e), e.name);
                                if let Some(size) = e.size {
                                    line.push_str(&format!(" — {}", format_bytes(size)));
                                }
                                ui.add(egui::Label::new(line).truncate());
                            }
                        });
                }
                // Liệt kê RIÊNG các tên không khớp (không chỉ đếm) để người
                // dùng thấy ngay tên nào gõ sai / không nằm trong thư mục
                // này, khỏi phải tự đối chiếu cả danh sách dài.
                if !self.bulk_list_unmatched.is_empty() {
                    ui.add_space(4.0);
                    ui.colored_label(
                        tone_color(ui, Tone::Warn),
                        "Các tên này không khớp mục nào (kiểm tra chính tả, đuôi file, chữ \
                         hoa/thường):",
                    );
                    let row_h = ui.text_style_height(&egui::TextStyle::Body);
                    let unmatched = &self.bulk_list_unmatched;
                    egui::ScrollArea::vertical()
                        .id_salt("bulk_list_unmatched_scroll")
                        .max_height(80.0)
                        .show_rows(ui, row_h, unmatched.len(), |ui, row_range| {
                            for i in row_range {
                                ui.add(egui::Label::new(format!("• {}", unmatched[i])).truncate());
                            }
                        });
                }

                if !self.bulk_list_matches.is_empty() {
                    ui.add_space(6.0);
                    // Tên khớp 1 THƯ MỤC thì cả thư mục (đệ quy) được tải —
                    // nên đếm là "mục", và ghi chú riêng số thư mục chưa
                    // biết dung lượng (giống nút "Tải N mục đã chọn").
                    let (known_bytes, unknown_folders) =
                        sum_known_size(self.bulk_list_matches.iter(), &self.folder_sizes);
                    let mut download_label = format!(
                        "⬇ Tải {} mục (~{})",
                        self.bulk_list_matches.len(),
                        format_bytes(known_bytes)
                    );
                    if unknown_folders > 0 {
                        download_label
                            .push_str(&format!(" +{unknown_folders} thư mục chưa rõ dung lượng"));
                    }
                    if ui
                        .add_enabled(
                            !busy,
                            egui::Button::new(download_label)
                                .min_size(vec2(ui.available_width(), 28.0)),
                        )
                        .clicked()
                    {
                        download_clicked = true;
                    }

                    if logged_in {
                        ui.add_space(6.0);
                        ui.separator();
                        ui.weak("Hoặc xóa các mục khớp khỏi Drive:");
                        // Hỏi 1 LẦN DUY NHẤT trước khi bắt đầu, áp dụng cho MỌI
                        // file không sở hữu gặp trong lượt này — không dừng lại
                        // hỏi thêm giữa chừng theo từng file nữa (vừa chậm vì
                        // phải kiểm tra hết mới hỏi, vừa không cần thiết: file
                        // luôn còn nguyên trong Thùng rác hoặc thư mục tạm,
                        // không mất gì nên không có nhiều rủi ro phải cân nhắc).
                        ui.add_enabled(
                            !busy,
                            egui::Checkbox::new(
                                &mut self.bulk_delete_move_aside_agreed,
                                "Cho phép chuyển file không sở hữu vào thư mục tạm",
                            ),
                        );
                        let label =
                            format!("🗑 Chuyển {} file vào Thùng rác", self.bulk_list_matches.len());
                        if ui
                            .add_enabled(
                                !busy,
                                egui::Button::new(label)
                                    .min_size(vec2(ui.available_width(), 28.0)),
                            )
                            .clicked()
                        {
                            delete_clicked = true;
                        }
                    }
                }
            });

        if find_clicked {
            self.find_bulk_list_matches();
        }
        if download_clicked {
            let entries = self.bulk_list_matches.clone();
            self.start_download(entries);
        }
        if delete_clicked {
            let entries = self.bulk_list_matches.clone();
            let allow_move_aside = self.bulk_delete_move_aside_agreed;
            self.start_bulk_delete(entries, allow_move_aside);
        }
    }
}

impl eframe::App for GDriveCopierApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.drain_events();
        self.update_speed_estimate();
        if self.is_logged_in() {
            self.handle_dropped_files(ui.ctx());
        }

        let ready = self.config.api_key.is_some();
        // Chưa có API key thì BẮT BUỘC ở trang Cài đặt (chưa làm được gì khác).
        let settings_open = self.show_settings || !ready;

        // Thứ tự thêm khung quan trọng: khung thêm trước nằm ngoài cùng,
        // CentralPanel luôn thêm cuối cùng.
        egui::Panel::top("top_bar").show(ui, |ui| {
            self.draw_top_bar(ui);
        });

        if ready {
            if !settings_open {
                egui::Panel::bottom("bottom_bar").show(ui, |ui| {
                    self.draw_bottom_bar(ui);
                });
            }
            egui::Panel::right("activity_panel")
                .resizable(true)
                .default_size(340.0)
                .size_range(280.0..=640.0)
                .show(ui, |ui| {
                    // Khung kéo giãn được thì nội dung phải chiếm hết chỗ trống,
                    // nếu không khung sẽ co lại theo nội dung ở khung hình sau.
                    ui.take_available_space();
                    self.draw_activity_panel(ui);
                });
        }

        egui::CentralPanel::default().show(ui, |ui| {
            if settings_open {
                self.draw_settings_page(ui);
            } else {
                self.draw_browser(ui);
            }
        });

        if self.is_busy() {
            ui.ctx()
                .request_repaint_after(std::time::Duration::from_millis(80));
        }
    }
}

// ---------------------------------------------------------------------------
// Thành phần giao diện dùng chung
// ---------------------------------------------------------------------------

/// Màu nhấn cho nút chính ("Tải đã chọn").
const ACCENT: Color32 = Color32::from_rgb(45, 108, 223);

/// Chiều cao 1 dòng trong bảng file — CỐ ĐỊNH để `ScrollArea::show_rows` chỉ
/// vẽ đúng các dòng đang nhìn thấy (thư mục vài nghìn file vẫn mượt).
const ROW_H: f32 = 26.0;
const COL_CHECK_W: f32 = 30.0;
const COL_SIZE_W: f32 = 150.0;
const COL_ACTIONS_W: f32 = 118.0;
const COL_GAP: f32 = 8.0;
const PAD_LEFT: f32 = 8.0;
/// Chừa chỗ bên phải cho thanh cuộn nổi của egui, khỏi đè lên nút thao tác.
const PAD_RIGHT: f32 = 14.0;

/// Màu chữ theo ngữ nghĩa, chọn theo giao diện sáng/tối để luôn đủ tương phản.
#[derive(Clone, Copy)]
enum Tone {
    Ok,
    Error,
    Warn,
}

fn tone_color(ui: &egui::Ui, tone: Tone) -> Color32 {
    let dark = ui.visuals().dark_mode;
    match (tone, dark) {
        (Tone::Ok, true) => Color32::from_rgb(96, 176, 112),
        (Tone::Ok, false) => Color32::from_rgb(28, 128, 58),
        (Tone::Error, true) => Color32::from_rgb(224, 96, 96),
        (Tone::Error, false) => Color32::from_rgb(190, 40, 40),
        (Tone::Warn, true) => Color32::from_rgb(224, 176, 96),
        (Tone::Warn, false) => Color32::from_rgb(158, 100, 0),
    }
}

/// Khung thông báo: nền tô nhạt + viền cùng tông với màu chữ.
fn banner_frame(color: Color32) -> egui::Frame {
    egui::Frame::new()
        .fill(color.gamma_multiply(0.12))
        .stroke(egui::Stroke::new(1.0, color.gamma_multiply(0.55)))
        .corner_radius(4)
        .inner_margin(egui::Margin::symmetric(10, 6))
}

/// Thẻ có viền, chiếm hết chiều ngang của cột chứa nó (dùng ở trang Cài đặt).
fn card<R>(ui: &mut egui::Ui, add_contents: impl FnOnce(&mut egui::Ui) -> R) -> R {
    egui::Frame::group(ui.style())
        .inner_margin(egui::Margin::same(14))
        .show(ui, |ui| {
            ui.set_width(ui.available_width());
            add_contents(ui)
        })
        .inner
}

/// Đặt widget vào 1 hình chữ nhật có sẵn (không cấp phát thêm chỗ trong
/// `ui` cha — chỗ đã được cấp bằng `allocate_exact_size` từ trước). Dùng để
/// dàn các "ô" theo cột cố định trong 1 dòng của bảng.
fn cell<R>(
    ui: &mut egui::Ui,
    rect: Rect,
    layout: Layout,
    add_contents: impl FnOnce(&mut egui::Ui) -> R,
) -> R {
    let mut child = ui.new_child(egui::UiBuilder::new().max_rect(rect).layout(layout));
    add_contents(&mut child)
}

/// Các cột của 1 dòng trong bảng file.
struct Cols {
    check: Rect,
    name: Rect,
    size: Rect,
    actions: Rect,
}

/// Chia 1 dòng thành các cột: [tick][tên — giãn][dung lượng][thao tác].
fn table_cols(row: Rect) -> Cols {
    let left = row.left() + PAD_LEFT;
    let right = (row.right() - PAD_RIGHT).max(left);
    let (top, bottom) = (row.top(), row.bottom());
    let actions_left = (right - COL_ACTIONS_W).max(left);
    let size_left = (actions_left - COL_SIZE_W).max(left);
    let check_right = left + COL_CHECK_W;
    Cols {
        check: Rect::from_min_max(pos2(left, top), pos2(check_right, bottom)),
        name: Rect::from_min_max(
            pos2(check_right, top),
            pos2((size_left - COL_GAP).max(check_right + 40.0), bottom),
        ),
        size: Rect::from_min_max(
            pos2(size_left, top),
            pos2((actions_left - COL_GAP).max(size_left), bottom),
        ),
        actions: Rect::from_min_max(pos2(actions_left, top), pos2(right, bottom)),
    }
}

/// Mọi thao tác người dùng phát sinh trong lúc vẽ bảng — gom lại rồi mới
/// áp lên `GDriveCopierApp` sau khi vẽ xong (xem `draw_file_table`).
#[derive(Default)]
struct TableActions {
    navigate_to: Option<(String, String)>,
    download_single: Option<DriveEntry>,
    compute_size_for: Option<String>,
    /// (vị trí trong danh sách, có giữ Shift không, trạng thái tick mới)
    selection_change: Option<(usize, bool, bool)>,
    toggle_select_all: Option<bool>,
    start_rename_for: Option<(String, String)>,
    rename_confirmed: Option<(String, String)>,
    rename_cancelled: bool,
    delete_clicked: Option<DriveEntry>,
}

/// Dữ liệu chỉ-đọc dùng chung cho mọi dòng của bảng.
struct RowEnv<'a> {
    selected: &'a HashSet<String>,
    folder_sizes: &'a HashMap<String, FolderSizeState>,
    can_download: bool,
    logged_in: bool,
}

fn draw_table_header(
    ui: &mut egui::Ui,
    all_selected: bool,
    partially_selected: bool,
    actions: &mut TableActions,
) {
    let (row, _) = ui.allocate_exact_size(vec2(ui.available_width(), ROW_H), Sense::hover());
    let cols = table_cols(row);
    let stroke = ui.visuals().widgets.noninteractive.bg_stroke;
    ui.painter().hline(row.x_range(), row.bottom(), stroke);

    cell(ui, cols.check, Layout::left_to_right(Align::Center), |ui| {
        let mut master = all_selected;
        let resp = ui.add(egui::Checkbox::without_text(&mut master).indeterminate(partially_selected));
        if resp.changed() {
            // Đang chọn dở (indeterminate) mà bấm -> bỏ chọn hết, giống Gmail.
            actions.toggle_select_all = Some(master && !partially_selected);
        }
        resp.on_hover_text("Chọn tất cả / bỏ chọn tất cả");
    });
    cell(ui, cols.name, Layout::left_to_right(Align::Center), |ui| {
        ui.label(RichText::new("Tên").weak().small());
    });
    cell(ui, cols.size, Layout::right_to_left(Align::Center), |ui| {
        ui.label(RichText::new("Dung lượng").weak().small());
    });
}

fn draw_entry_row(
    ui: &mut egui::Ui,
    i: usize,
    entry: &DriveEntry,
    env: &RowEnv,
    renaming: &mut Option<(String, String)>,
    rename_focus: &mut bool,
    actions: &mut TableActions,
) {
    let (row, _) = ui.allocate_exact_size(vec2(ui.available_width(), ROW_H), Sense::hover());
    let cols = table_cols(row);
    let is_selected = env.selected.contains(&entry.id);
    let hovered = ui.rect_contains_pointer(row);
    let is_renaming_this =
        env.logged_in && renaming.as_ref().is_some_and(|(id, _)| id == &entry.id);

    // Nền dòng: đang chọn > đang trỏ chuột > kẻ sọc xen kẽ.
    let bg = if is_selected {
        Some(ui.visuals().selection.bg_fill.gamma_multiply(0.45))
    } else if hovered {
        Some(ui.visuals().widgets.hovered.weak_bg_fill.gamma_multiply(0.7))
    } else if i % 2 == 1 {
        Some(ui.visuals().faint_bg_color)
    } else {
        None
    };
    if let Some(bg) = bg {
        ui.painter().rect_filled(row, egui::CornerRadius::ZERO, bg);
    }

    // --- Ô tick chọn
    cell(ui, cols.check, Layout::left_to_right(Align::Center), |ui| {
        let mut checked = is_selected;
        if ui.checkbox(&mut checked, "").changed() {
            let shift_held = ui.input(|inp| inp.modifiers.shift);
            actions.selection_change = Some((i, shift_held, checked));
        }
    });

    // --- Ô tên (hoặc ô nhập tên mới khi đang đổi tên)
    cell(ui, cols.name, Layout::left_to_right(Align::Center), |ui| {
        if is_renaming_this {
            let edit_w = (ui.available_width() - 70.0).max(80.0);
            let mut confirm: Option<String> = None;
            let mut cancel = false;
            if let Some((_, draft)) = renaming.as_mut() {
                let resp = ui.add(
                    egui::TextEdit::singleline(draft)
                        .id_salt(("rename_edit", entry.id.as_str()))
                        .desired_width(edit_w),
                );
                if *rename_focus {
                    resp.request_focus();
                    *rename_focus = false;
                }
                let enter = resp.lost_focus() && ui.input(|inp| inp.key_pressed(egui::Key::Enter));
                let esc = (resp.has_focus() || resp.lost_focus())
                    && ui.input(|inp| inp.key_pressed(egui::Key::Escape));
                let ok_clicked = ui.button("✓").clicked();
                let cancel_clicked = ui.button("✕").clicked();
                if ok_clicked || enter {
                    confirm = Some(draft.clone());
                }
                if cancel_clicked || esc {
                    cancel = true;
                }
            }
            if let Some(new_name) = confirm {
                actions.rename_confirmed = Some((entry.id.clone(), new_name));
            }
            if cancel {
                actions.rename_cancelled = true;
            }
        } else {
            let name_resp = ui.add(
                egui::Label::new(format!("{}  {}", entry_icon(entry), entry.name))
                    .truncate()
                    .sense(Sense::click())
                    .show_tooltip_when_elided(false),
            );
            if entry.is_folder {
                if name_resp.double_clicked() {
                    actions.navigate_to = Some((entry.id.clone(), entry.name.clone()));
                }
                name_resp.on_hover_text(format!("{}\nNhấp đúp để mở thư mục này", entry.name));
            } else {
                name_resp.on_hover_text(entry.name.as_str());
            }
        }
    });

    // --- Ô dung lượng
    cell(ui, cols.size, Layout::right_to_left(Align::Center), |ui| {
        if let Some(size) = entry.size {
            ui.weak(format_bytes(size));
        } else if entry.is_folder {
            match env.folder_sizes.get(&entry.id) {
                Some(FolderSizeState::Known { files, bytes }) => {
                    ui.add(
                        egui::Label::new(
                            RichText::new(format!("{files} mục • {}", format_bytes(*bytes))).weak(),
                        )
                        .truncate(),
                    );
                }
                Some(FolderSizeState::Computing) => {
                    ui.spinner();
                }
                Some(FolderSizeState::Error) => {
                    if ui.link("lỗi — thử lại").clicked() {
                        actions.compute_size_for = Some(entry.id.clone());
                    }
                }
                None => {
                    if ui.link("xem dung lượng").clicked() {
                        actions.compute_size_for = Some(entry.id.clone());
                    }
                }
            }
        } else {
            // File Google Docs/Sheets/Slides: Google không cho biết trước
            // dung lượng khi xuất ra.
            ui.weak("—");
        }
    });

    // --- Ô thao tác (thêm từ phải sang trái: Tải luôn sát mép phải)
    cell(ui, cols.actions, Layout::right_to_left(Align::Center), |ui| {
        if is_renaming_this {
            return;
        }
        if ui
            .add_enabled(
                env.can_download,
                egui::Button::new("Tải").min_size(vec2(44.0, 22.0)),
            )
            .on_hover_text("Tải mục này")
            .clicked()
        {
            actions.download_single = Some(entry.clone());
        }
        if env.logged_in {
            if ui
                .add(egui::Button::new("🗑").frame_when_inactive(false))
                .on_hover_text("Chuyển vào Thùng rác")
                .clicked()
            {
                actions.delete_clicked = Some(entry.clone());
            }
            if ui
                .add(egui::Button::new("✎").frame_when_inactive(false))
                .on_hover_text("Đổi tên")
                .clicked()
            {
                actions.start_rename_for = Some((entry.id.clone(), entry.name.clone()));
            }
        }
    });
}

/// Biểu tượng theo loại file để dễ quét mắt qua danh sách dài.
fn entry_icon(entry: &DriveEntry) -> &'static str {
    if entry.is_folder {
        return "📁";
    }
    let mime = entry.mime_type.as_str();
    if mime.starts_with("image/") {
        "🖼"
    } else if mime.starts_with("video/") {
        "🎬"
    } else if mime.starts_with("audio/") {
        "🎵"
    } else if mime.contains("zip")
        || mime.contains("compressed")
        || mime.contains("tar")
        || mime.contains("rar")
    {
        "📦"
    } else {
        "📄"
    }
}

/// (số mục đang chọn, tổng dung lượng đã biết, số thư mục chưa rõ dung lượng).
fn selection_summary(
    entries: &[DriveEntry],
    selected: &HashSet<String>,
    folder_sizes: &HashMap<String, FolderSizeState>,
) -> (usize, u64, usize) {
    let count = entries.iter().filter(|e| selected.contains(&e.id)).count();
    if count == 0 {
        return (0, 0, 0);
    }
    let (bytes, unknown_folders) = sum_known_size(
        entries.iter().filter(|e| selected.contains(&e.id)),
        folder_sizes,
    );
    (count, bytes, unknown_folders)
}

/// Tô màu dòng nhật ký theo ký hiệu đầu dòng (✔ xong, ✘ lỗi, ↻/↪ cảnh báo...).
fn log_line_text(ui: &egui::Ui, line: &str) -> RichText {
    let text = RichText::new(line).monospace().size(12.0);
    match line.chars().next() {
        Some('✔') => text.color(tone_color(ui, Tone::Ok)),
        Some('✘') => text.color(tone_color(ui, Tone::Error)),
        Some('↻') | Some('↪') => text.color(tone_color(ui, Tone::Warn)),
        Some('⏭') => text.weak(),
        _ => text,
    }
}

/// Mật độ hiển thị cho màn hình máy tính: khoảng cách và đệm nút vừa phải,
/// dễ bấm bằng chuột nhưng vẫn gọn.
fn setup_style(ctx: &egui::Context) {
    ctx.all_styles_mut(|style| {
        style.spacing.item_spacing = vec2(8.0, 6.0);
        style.spacing.button_padding = vec2(10.0, 4.0);
        style.spacing.interact_size.y = 24.0;
    });
}

/// Đảm bảo access_token còn hiệu lực (tự làm mới bằng refresh_token nếu đã
/// hết hạn), báo cho GUI biết qua `TokensRefreshed` để lưu lại vào cấu hình
/// nếu có thay đổi. Dùng chung cho mọi thao tác ghi (đổi tên/xóa).
async fn ensure_fresh_and_notify(
    http: &reqwest::Client,
    client_id: &str,
    client_secret: &str,
    tokens: OAuthTokens,
    tx: &UnboundedSender<WorkerEvent>,
) -> anyhow::Result<String> {
    let fresh = crate::oauth::ensure_fresh(http, client_id, client_secret, tokens.clone()).await?;
    if fresh.access_token != tokens.access_token {
        let _ = tx.send(WorkerEvent::TokensRefreshed(fresh.clone()));
    }
    Ok(fresh.access_token)
}

fn set_selected(selected: &mut HashSet<String>, id: &str, checked: bool) {
    if checked {
        selected.insert(id.to_string());
    } else {
        selected.remove(id);
    }
}

/// Cộng dồn dung lượng đã BIẾT của 1 danh sách mục: file thì lấy `size`
/// thẳng, thư mục thì lấy từ cache `folder_sizes` nếu đã tính; trả về thêm
/// số thư mục CHƯA rõ dung lượng để hiển thị minh bạch (không âm thầm coi
/// như 0 byte).
fn sum_known_size<'a>(
    entries: impl Iterator<Item = &'a DriveEntry>,
    folder_sizes: &HashMap<String, FolderSizeState>,
) -> (u64, usize) {
    let mut bytes = 0u64;
    let mut unknown_folders = 0usize;
    for e in entries {
        if let Some(sz) = e.size {
            bytes += sz;
        } else if e.is_folder {
            match folder_sizes.get(&e.id) {
                Some(FolderSizeState::Known { bytes: b, .. }) => bytes += b,
                _ => unknown_folders += 1,
            }
        }
    }
    (bytes, unknown_folders)
}

/// Tách văn bản người dùng dán/nhập vào thành các "đoạn" theo dấu phân cách
/// RÕ RÀNG (xuống dòng, phẩy, chấm phẩy, tab) — chưa vội tách theo khoảng
/// trắng, vì tên file hợp lệ có thể tự nó chứa khoảng trắng.
fn split_name_list(input: &str) -> Vec<String> {
    input
        .split(|c: char| c == '\n' || c == '\r' || c == ',' || c == ';' || c == '\t')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Thêm vào `matched` MỌI mục trong `entries` có tên đúng bằng `name` (Drive
/// cho phép nhiều file cùng tên trong 1 thư mục) mà chưa có trong `matched`.
/// Trả về `true` nếu CÓ ít nhất 1 mục mang tên đó — kể cả khi mục đó đã được
/// thêm từ 1 dòng trước, để nhập trùng 1 tên 2 lần không bị báo nhầm là
/// "không khớp".
fn collect_by_name(
    entries: &[DriveEntry],
    name: &str,
    matched: &mut Vec<DriveEntry>,
    matched_ids: &mut HashSet<String>,
) -> bool {
    let mut found = false;
    for entry in entries {
        if entry.name == name {
            found = true;
            if matched_ids.insert(entry.id.clone()) {
                matched.push(entry.clone());
            }
        }
    }
    found
}

/// So khớp danh sách tên (đã tách bằng `split_name_list`) với các mục của 1
/// thư mục. Trả về `(các mục khớp, các tên không khớp mục nào)`.
///
/// Quy tắc: tên phải khớp CHÍNH XÁC (phân biệt hoa/thường). Ưu tiên khớp CẢ
/// ĐOẠN trước (đúng với tên file có khoảng trắng); nếu cả đoạn không khớp gì
/// và có khoảng trắng, mới thử tách tiếp theo khoảng trắng (đúng với nhiều
/// tên file đơn giản cách nhau bằng dấu cách trên cùng 1 dòng) — khi đó từng
/// tên không khớp được báo riêng. Nếu không tên con nào khớp, báo nguyên cả
/// đoạn (giữ đúng chữ người dùng đã nhập). Các mục khớp được trả về theo thứ
/// tự xuất hiện trong danh sách nhập, không lặp lại dù 1 tên bị nhập nhiều lần.
fn match_names_in_folder(
    entries: &[DriveEntry],
    segments: &[String],
) -> (Vec<DriveEntry>, Vec<String>) {
    let mut matched: Vec<DriveEntry> = Vec::new();
    let mut matched_ids: HashSet<String> = HashSet::new();
    let mut unmatched: Vec<String> = Vec::new();

    for segment in segments {
        if collect_by_name(entries, segment, &mut matched, &mut matched_ids) {
            continue;
        }
        if segment.contains(char::is_whitespace) {
            let mut missing: Vec<&str> = Vec::new();
            let mut any_token_found = false;
            for token in segment.split_whitespace() {
                if collect_by_name(entries, token, &mut matched, &mut matched_ids) {
                    any_token_found = true;
                } else {
                    missing.push(token);
                }
            }
            if any_token_found {
                unmatched.extend(missing.into_iter().map(|t| t.to_string()));
                continue;
            }
        }
        unmatched.push(segment.clone());
    }
    (matched, unmatched)
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    if bytes == 0 {
        return "0 B".to_string();
    }
    let mut size = bytes as f64;
    let mut unit_idx = 0;
    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{bytes} B")
    } else {
        format!("{size:.1} {}", UNITS[unit_idx])
    }
}

fn format_speed(bytes_per_sec: f64) -> String {
    if bytes_per_sec < 1024.0 {
        return "-- /giây".to_string();
    }
    format!("{}/giây", format_bytes(bytes_per_sec as u64))
}

fn format_duration(seconds: f64) -> String {
    let total_secs = seconds.round().max(0.0) as u64;
    if total_secs < 60 {
        format!("{total_secs} giây")
    } else if total_secs < 3600 {
        format!("{} phút {} giây", total_secs / 60, total_secs % 60)
    } else {
        format!("{} giờ {} phút", total_secs / 3600, (total_secs % 3600) / 60)
    }
}

/// egui không có sẵn font hỗ trợ dấu tiếng Việt, nên chữ có dấu sẽ hiện
/// thành ô vuông (□). Nhúng thẳng Noto Sans (SIL OFL, xem assets/) vào ứng
/// dụng để không phụ thuộc máy người dùng có cài font gì.
fn setup_vietnamese_font(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    fonts.font_data.insert(
        "noto_sans_vn".to_owned(),
        std::sync::Arc::new(egui::FontData::from_static(include_bytes!(
            "../assets/NotoSans-Regular.ttf"
        ))),
    );

    // Chữ thường (label, nút, tiêu đề...): ưu tiên cao nhất để mọi dấu tiếng
    // Việt hiển thị đúng ngay từ đầu.
    if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
        family.insert(0, "noto_sans_vn".to_owned());
    }
    // Monospace (đường dẫn, nhật ký...): vẫn ưu tiên font monospace mặc định
    // của egui để giữ căn cột đều; Noto Sans chỉ dùng làm phương án dự
    // phòng cho ký tự có dấu mà font monospace mặc định không có.
    if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Monospace) {
        family.push("noto_sans_vn".to_owned());
    }

    ctx.set_fonts(fonts);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, name: &str) -> DriveEntry {
        DriveEntry {
            id: id.to_string(),
            name: name.to_string(),
            mime_type: "application/octet-stream".to_string(),
            size: Some(1),
            is_folder: false,
        }
    }

    fn names(entries: &[DriveEntry]) -> Vec<&str> {
        entries.iter().map(|e| e.name.as_str()).collect()
    }

    #[test]
    fn splits_on_newline_comma_semicolon_and_tab_but_keeps_inner_spaces() {
        let got = split_name_list("a.jpg, b.mp4\nc.png; d.pdf\te.txt\r\n  ảnh cưới.jpg  ");
        assert_eq!(
            got,
            vec!["a.jpg", "b.mp4", "c.png", "d.pdf", "e.txt", "ảnh cưới.jpg"]
        );
    }

    #[test]
    fn blank_input_yields_no_segments() {
        assert!(split_name_list("  \n ,; \t\r\n").is_empty());
    }

    #[test]
    fn matches_in_list_order_and_ignores_repeated_names() {
        let entries = vec![entry("1", "a.jpg"), entry("2", "b.jpg"), entry("3", "c.jpg")];
        let segments = split_name_list("c.jpg\na.jpg\nc.jpg");
        let (matched, unmatched) = match_names_in_folder(&entries, &segments);
        assert_eq!(names(&matched), vec!["c.jpg", "a.jpg"]);
        assert!(
            unmatched.is_empty(),
            "nhập trùng 1 tên 2 lần không được báo là 'không khớp'"
        );
    }

    #[test]
    fn reports_each_name_that_matches_nothing() {
        let entries = vec![entry("1", "a.jpg")];
        let segments = split_name_list("a.jpg\nzzz.jpg\nA.JPG");
        let (matched, unmatched) = match_names_in_folder(&entries, &segments);
        assert_eq!(names(&matched), vec!["a.jpg"]);
        assert_eq!(
            unmatched,
            vec!["zzz.jpg", "A.JPG"],
            "khớp phân biệt hoa/thường"
        );
    }

    #[test]
    fn whole_segment_with_spaces_wins_over_whitespace_split() {
        let entries = vec![
            entry("1", "ảnh cưới.jpg"),
            entry("2", "ảnh.jpg"),
            entry("3", "cưới.jpg"),
        ];
        let (matched, unmatched) =
            match_names_in_folder(&entries, &split_name_list("ảnh cưới.jpg"));
        assert_eq!(names(&matched), vec!["ảnh cưới.jpg"]);
        assert!(unmatched.is_empty());
    }

    #[test]
    fn falls_back_to_whitespace_split_and_reports_only_the_missing_tokens() {
        let entries = vec![entry("1", "a.jpg"), entry("2", "b.jpg")];
        let (matched, unmatched) =
            match_names_in_folder(&entries, &split_name_list("a.jpg b.jpg c.jpg"));
        assert_eq!(names(&matched), vec!["a.jpg", "b.jpg"]);
        assert_eq!(unmatched, vec!["c.jpg"]);
    }

    #[test]
    fn segment_with_spaces_matching_nothing_is_reported_verbatim() {
        let entries = vec![entry("1", "a.jpg")];
        let (matched, unmatched) =
            match_names_in_folder(&entries, &split_name_list("ảnh cưới.jpg"));
        assert!(matched.is_empty());
        assert_eq!(unmatched, vec!["ảnh cưới.jpg"]);
    }

    #[test]
    fn several_entries_sharing_one_name_are_all_matched() {
        // Drive cho phép 2 file cùng tên nằm trong cùng 1 thư mục.
        let entries = vec![entry("1", "a.jpg"), entry("2", "a.jpg")];
        let (matched, unmatched) = match_names_in_folder(&entries, &split_name_list("a.jpg"));
        assert_eq!(matched.len(), 2);
        assert!(unmatched.is_empty());
    }
}
