//! Giao diện người dùng (egui/eframe). File này chỉ lo việc HIỂN THỊ và điều
//! phối: mọi việc gọi mạng thật sự nằm ở `drive_api.rs` / `downloader.rs`,
//! chạy trên 1 tokio runtime nền, kết quả gửi về đây qua kênh (channel) rồi
//! mới cập nhật lên state để vẽ lại UI ở khung hình kế tiếp.

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

    runtime: tokio::runtime::Runtime,
    event_tx: UnboundedSender<WorkerEvent>,
    event_rx: UnboundedReceiver<WorkerEvent>,
}

impl GDriveCopierApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        setup_vietnamese_font(&cc.egui_ctx);

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

    /// Trạng thái đăng nhập Google, hiện Ở GÓC PHẢI thanh tiêu đề — LUÔN
    /// thấy được (không cần mở Cài đặt) để biết ngay đang thao tác bằng
    /// tài khoản nào, quan trọng vì việc xóa phụ thuộc đúng tài khoản đang
    /// đăng nhập (xem `ConfirmDeleteState`).
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
            ui.colored_label(egui::Color32::from_rgb(96, 176, 112), format!("✔ {who}"));
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

    fn draw_settings(&mut self, ui: &mut egui::Ui) {
        ui.label(
            "Cần có Google API key (miễn phí) để app đọc được dữ liệu Drive công khai \
             mà không bắt bạn đăng nhập. Xem hướng dẫn lấy key trong README.md đi kèm.",
        );
        ui.add_space(8.0);
        ui.horizontal(|ui| {
            ui.label("API key:");
            ui.add(
                egui::TextEdit::singleline(&mut self.api_key_input)
                    .password(true)
                    .desired_width(360.0),
            );
        });
        ui.add_space(8.0);
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

        ui.add_space(16.0);
        ui.separator();
        ui.add_space(8.0);
        ui.heading("Đổi tên / xóa file (không bắt buộc)");
        ui.label(
            "Chỉ cần nếu muốn đổi tên hoặc chuyển file vào Thùng rác trên thư mục đã chia sẻ \
             quyền chỉnh sửa. Không liên quan gì tới API key ở trên — cần đăng nhập Google 1 \
             lần. Xem hướng dẫn tạo OAuth Client ID trong README.md.",
        );
        ui.add_space(8.0);
        ui.horizontal(|ui| {
            ui.label("OAuth Client ID:");
            ui.add(egui::TextEdit::singleline(&mut self.oauth_client_id_input).desired_width(360.0));
        });
        ui.horizontal(|ui| {
            ui.label("OAuth Client Secret:");
            ui.add(
                egui::TextEdit::singleline(&mut self.oauth_client_secret_input)
                    .password(true)
                    .desired_width(360.0),
            );
        });
        ui.add_space(8.0);

        if self.is_logged_in() {
            let who = self
                .config
                .oauth_email
                .clone()
                .unwrap_or_else(|| "(không rõ email)".to_string());
            ui.horizontal(|ui| {
                ui.colored_label(
                    egui::Color32::from_rgb(96, 176, 112),
                    format!("✔ Đã đăng nhập: {who}"),
                );
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
    }

    /// Link Drive + thư mục lưu + chính sách trùng tên gộp CHUNG 1 dòng
    /// (tự xuống dòng nếu cửa sổ hẹp) — khung dán link không cần rộng vì
    /// dán xong là xong, không cần đọc lại, nên nhường chỗ ngang cho 2 mục
    /// còn lại thay vì mỗi mục chiếm nguyên 1 dòng riêng.
    fn draw_link_bar(&mut self, ui: &mut egui::Ui) {
        let busy = self.is_busy();
        ui.horizontal_wrapped(|ui| {
            ui.label("Link Drive:");
            let resp = ui.add_enabled(
                !busy,
                egui::TextEdit::singleline(&mut self.link_input)
                    .desired_width(220.0)
                    .hint_text("https://drive.google.com/drive/folders/..."),
            );
            let enter_pressed = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
            let open_clicked = ui.add_enabled(!busy, egui::Button::new("Mở")).clicked();
            if enter_pressed || open_clicked {
                self.start_open_link();
            }
            if ui.button("⚙").on_hover_text("Cài đặt API key").clicked() {
                self.show_settings = true;
            }

            ui.separator();

            ui.label("Lưu vào:");
            let text = self
                .destination
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "(chưa chọn)".to_string());
            ui.monospace(text);
            if ui.button("Chọn thư mục...").clicked() {
                self.start_pick_folder();
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
    }

    fn draw_breadcrumbs(&mut self, ui: &mut egui::Ui) {
        if self.breadcrumbs.is_empty() {
            return;
        }
        let busy = self.is_busy();
        let mut go_to: Option<usize> = None;
        ui.horizontal_wrapped(|ui| {
            let last_idx = self.breadcrumbs.len() - 1;
            for (i, (_, name)) in self.breadcrumbs.iter().enumerate() {
                if i > 0 {
                    ui.label("›");
                }
                if i == last_idx {
                    ui.strong(name);
                } else if ui.add_enabled(!busy, egui::Button::new(name)).clicked() {
                    go_to = Some(i);
                }
            }
        });
        if let Some(i) = go_to {
            self.navigate_to_breadcrumb(i);
        }
    }

    fn draw_entry_list(&mut self, ui: &mut egui::Ui) {
        if self.current_entries.is_empty() {
            ui.label("Dán link một thư mục Google Drive công khai ở trên rồi bấm \"Mở\".");
            return;
        }

        let busy = self.is_busy();
        let can_download = !busy;
        let logged_in = self.is_logged_in();
        let entries = self.current_entries.clone();
        let folder_sizes = self.folder_sizes.clone();
        let selected_snapshot = self.selected.clone();
        let renaming_snapshot = self.renaming.clone();
        let mut renaming_draft = renaming_snapshot
            .as_ref()
            .map(|(_, t)| t.clone())
            .unwrap_or_default();
        let mut navigate_to: Option<(String, String)> = None;
        let mut download_single: Option<DriveEntry> = None;
        let mut compute_size_for: Option<String> = None;
        // (vị trí trong `entries`, có giữ Shift không, trạng thái tick mới)
        let mut selection_change: Option<(usize, bool, bool)> = None;
        let mut toggle_select_all: Option<bool> = None;
        let mut start_rename_for: Option<(String, String)> = None;
        let mut rename_confirmed: Option<(String, String)> = None;
        let mut rename_cancelled = false;
        let mut delete_clicked: Option<DriveEntry> = None;

        ui.horizontal(|ui| {
            let all_selected =
                !entries.is_empty() && entries.iter().all(|e| selected_snapshot.contains(&e.id));
            let mut master = all_selected;
            if ui.checkbox(&mut master, "Chọn tất cả").changed() {
                toggle_select_all = Some(master);
            }
            ui.label(format!("{} mục", entries.len()));
        });
        if logged_in {
            ui.weak(
                "Nhấp đúp vào tên thư mục để mở. Giữ Shift khi tick để chọn nhanh cả khoảng. \
                 ✎ đổi tên, 🗑 chuyển vào Thùng rác.",
            );
        } else {
            ui.weak("Nhấp đúp vào tên thư mục để mở. Giữ Shift khi tick để chọn nhanh cả khoảng.");
        }

        egui::ScrollArea::vertical().max_height(260.0).show_rows(
            ui,
            24.0,
            entries.len(),
            |ui, row_range| {
                for i in row_range {
                    let entry = &entries[i];
                    let is_renaming_this = logged_in
                        && renaming_snapshot
                            .as_ref()
                            .map(|(id, _)| id == &entry.id)
                            .unwrap_or(false);
                    ui.horizontal(|ui| {
                        let mut checked = selected_snapshot.contains(&entry.id);
                        if ui.checkbox(&mut checked, "").changed() {
                            let shift_held = ui.input(|inp| inp.modifiers.shift);
                            selection_change = Some((i, shift_held, checked));
                        }

                        if is_renaming_this {
                            let resp = ui.add(
                                egui::TextEdit::singleline(&mut renaming_draft)
                                    .desired_width(200.0),
                            );
                            let confirm_by_enter =
                                resp.lost_focus() && ui.input(|inp| inp.key_pressed(egui::Key::Enter));
                            if ui.button("✓").clicked() || confirm_by_enter {
                                rename_confirmed = Some((entry.id.clone(), renaming_draft.clone()));
                            }
                            if ui.button("✕").clicked() {
                                rename_cancelled = true;
                            }
                        } else {
                            let icon = if entry.is_folder { "📁" } else { "📄" };
                            let name_resp = ui.add(
                                egui::Label::new(format!("{icon} {}", entry.name))
                                    .sense(egui::Sense::click()),
                            );
                            if entry.is_folder {
                                if name_resp.double_clicked() {
                                    navigate_to = Some((entry.id.clone(), entry.name.clone()));
                                }
                                name_resp.on_hover_text("Nhấp đúp để mở thư mục này");
                            }

                            if let Some(size) = entry.size {
                                ui.weak(format_bytes(size));
                            } else if entry.is_folder {
                                match folder_sizes.get(&entry.id) {
                                    Some(FolderSizeState::Known { files, bytes }) => {
                                        ui.weak(format!("{files} mục • {}", format_bytes(*bytes)));
                                    }
                                    Some(FolderSizeState::Computing) => {
                                        ui.spinner();
                                    }
                                    Some(FolderSizeState::Error) => {
                                        ui.weak("(lỗi tính dung lượng)");
                                    }
                                    None => {
                                        if ui.link("xem dung lượng").clicked() {
                                            compute_size_for = Some(entry.id.clone());
                                        }
                                    }
                                }
                            }
                        }

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if !is_renaming_this
                                && ui
                                    .add_enabled(can_download, egui::Button::new("Tải"))
                                    .clicked()
                            {
                                download_single = Some(entry.clone());
                            }
                            if logged_in && !is_renaming_this {
                                if ui
                                    .button("🗑")
                                    .on_hover_text("Chuyển vào Thùng rác")
                                    .clicked()
                                {
                                    delete_clicked = Some(entry.clone());
                                }
                                if ui.button("✎").on_hover_text("Đổi tên").clicked() {
                                    start_rename_for = Some((entry.id.clone(), entry.name.clone()));
                                }
                            }
                        });
                    });
                }
            },
        );

        // --- Áp mọi thay đổi vào self SAU KHI closure ở trên đã kết thúc
        // (tránh vay mượn `self` bên trong closure của show_rows).
        if let Some(select_all) = toggle_select_all {
            if select_all {
                for e in &entries {
                    self.selected.insert(e.id.clone());
                }
            } else {
                self.selected.clear();
            }
        }
        if let Some((index, shift_held, checked)) = selection_change {
            if shift_held {
                if let Some(anchor) = self.selection_anchor {
                    let (lo, hi) = (anchor.min(index), anchor.max(index));
                    for e in &entries[lo..=hi] {
                        self.selected.insert(e.id.clone());
                    }
                } else {
                    // Chưa có điểm neo (lần tick đầu tiên) -> xử lý như tick đơn.
                    set_selected(&mut self.selected, &entries[index].id, checked);
                    self.selection_anchor = Some(index);
                }
            } else {
                set_selected(&mut self.selected, &entries[index].id, checked);
                self.selection_anchor = Some(index);
            }
        }
        if let Some(pair) = start_rename_for {
            self.renaming = Some(pair);
        }
        if rename_cancelled {
            self.renaming = None;
        }
        if let Some((id, new_name)) = rename_confirmed {
            self.start_rename(id, new_name);
        }
        if let Some(entry) = delete_clicked {
            self.start_check_ownership(entry);
        }

        if let Some(state) = self.confirm_delete.clone() {
            ui.add_space(4.0);
            match state {
                ConfirmDeleteState::CheckingOwnership(entry) => {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label(format!("Đang kiểm tra quyền sở hữu '{}'...", entry.name));
                    });
                }
                ConfirmDeleteState::NotOwned {
                    entry,
                    owner_label,
                    parent_id,
                } => {
                    let owner = owner_label.as_deref().unwrap_or("không rõ");
                    ui.colored_label(
                        egui::Color32::from_rgb(224, 176, 96),
                        format!(
                            "'{}' không phải của bạn (chủ sở hữu: {owner}) nên Google không \
                             cho xóa thẳng.",
                            entry.name
                        ),
                    );
                    ui.checkbox(&mut self.confirm_delete_move_aside, "Cho phép chuyển vào thư mục tạm");
                    ui.horizontal(|ui| {
                        let can_proceed = self.confirm_delete_move_aside && parent_id.is_some();
                        if ui.add_enabled(can_proceed, egui::Button::new("Xóa")).clicked() {
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
                }
            }
        }

        if let Some(entry) = download_single {
            self.start_download(vec![entry]);
        }
        if let Some((id, name)) = navigate_to {
            self.navigate_into(id, name);
        }
        if let Some(folder_id) = compute_size_for {
            self.start_compute_folder_size(folder_id);
        }

        ui.add_space(4.0);
        let mut download_all_clicked = false;
        let mut download_selected: Option<Vec<DriveEntry>> = None;
        ui.horizontal(|ui| {
            if ui
                .add_enabled(can_download, egui::Button::new("⬇ Tải tất cả mục đang hiển thị"))
                .clicked()
            {
                download_all_clicked = true;
            }

            if !selected_snapshot.is_empty() {
                let sel: Vec<DriveEntry> = entries
                    .iter()
                    .filter(|e| selected_snapshot.contains(&e.id))
                    .cloned()
                    .collect();
                let (known_bytes, unknown_folders) = sum_known_size(&sel, &folder_sizes);
                let mut label = format!(
                    "⬇ Tải {} mục đã chọn (~{})",
                    sel.len(),
                    format_bytes(known_bytes)
                );
                if unknown_folders > 0 {
                    label.push_str(&format!(" +{unknown_folders} thư mục chưa rõ dung lượng"));
                }
                if ui
                    .add_enabled(can_download, egui::Button::new(label))
                    .clicked()
                {
                    download_selected = Some(sel);
                }
            }
        });

        if download_all_clicked {
            self.start_download(entries);
        }
        if let Some(sel) = download_selected {
            self.selected.clear();
            self.start_download(sel);
        }
    }

    /// Ô nhập danh sách tên file + xem trước các mục KHỚP trong thư mục đang
    /// xem. Cùng 1 danh sách dùng được cho 2 việc:
    /// - TẢI về máy: luôn có, KHÔNG cần đăng nhập (vẫn dùng API key như các
    ///   cách tải khác — xem `start_download`);
    /// - chuyển vào Thùng rác: chỉ hiện khi đã đăng nhập Google.
    fn draw_bulk_list_section(&mut self, ui: &mut egui::Ui) {
        // Kéo-thả 1 file .txt chứa danh sách vào cửa sổ app -> tự đọc nội
        // dung, NỐI THÊM vào ô nhập (không xóa nội dung đã gõ sẵn, để không
        // mất công nếu người dùng đã tự nhập một phần trước đó). Việc này
        // hoạt động bất kể khung này đang mở hay đang thu gọn.
        let dropped_files = ui.ctx().input(|i| i.raw.dropped_files.clone());
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
                    self.set_status(format!("Đã nạp danh sách từ file: {}", path.display()), false);
                }
                Err(e) => {
                    self.set_status(format!("Không đọc được file {}: {e}", path.display()), true);
                }
            }
        }

        let busy = self.is_busy();
        let logged_in = self.is_logged_in();
        let matches_snapshot = self.bulk_list_matches.clone();
        let unmatched_snapshot = self.bulk_list_unmatched.clone();
        let folder_sizes = self.folder_sizes.clone();
        let mut find_clicked = false;
        let mut download_clicked = false;
        let mut delete_clicked = false;

        egui::CollapsingHeader::new("Tải / xóa hàng loạt theo danh sách tên")
            .default_open(false)
            .show(ui, |ui| {
                ui.weak(
                    "Dán danh sách tên (mỗi tên 1 dòng, hoặc cách nhau bằng dấu phẩy/chấm \
                     phẩy/khoảng trắng), hoặc kéo-thả 1 file .txt vào cửa sổ. Tên phải khớp \
                     CHÍNH XÁC tên đang hiển thị trong thư mục đang xem. Tải thì không cần \
                     đăng nhập, xóa thì cần.",
                );
                // Giới hạn chiều cao hiển thị — không bọc thì egui tự giãn
                // ô nhập cao theo đúng số dòng nội dung, dán danh sách vài
                // trăm dòng sẽ chiếm hết cửa sổ, khó theo dõi các phần bên
                // dưới. Nội dung dài hơn khung vẫn cuộn được bình thường,
                // không mất chữ.
                egui::ScrollArea::vertical()
                    .id_salt("bulk_list_input_scroll")
                    .max_height(80.0)
                    .show(ui, |ui| {
                        ui.add(
                            egui::TextEdit::multiline(&mut self.bulk_list_input)
                                .desired_rows(3)
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

                if !matches_snapshot.is_empty() || !unmatched_snapshot.is_empty() {
                    ui.add_space(8.0);
                    ui.label(format!(
                        "Khớp {} mục, {} tên không khớp mục nào trong thư mục đang xem",
                        matches_snapshot.len(),
                        unmatched_snapshot.len()
                    ));
                }
                if !matches_snapshot.is_empty() {
                    egui::ScrollArea::vertical()
                        .id_salt("bulk_list_matches_scroll")
                        .max_height(120.0)
                        .show(ui, |ui| {
                            for e in &matches_snapshot {
                                let icon = if e.is_folder { "📁" } else { "📄" };
                                let mut line = format!("• {icon} {}", e.name);
                                if let Some(size) = e.size {
                                    line.push_str(&format!(" — {}", format_bytes(size)));
                                }
                                ui.label(line);
                            }
                        });
                }
                // Liệt kê RIÊNG các tên không khớp (không chỉ đếm) để người
                // dùng thấy ngay tên nào gõ sai / không nằm trong thư mục
                // này, khỏi phải tự đối chiếu cả danh sách dài.
                if !unmatched_snapshot.is_empty() {
                    ui.add_space(4.0);
                    ui.colored_label(
                        egui::Color32::from_rgb(224, 176, 96),
                        "Các tên này không khớp mục nào (kiểm tra chính tả, đuôi file, chữ \
                         hoa/thường):",
                    );
                    egui::ScrollArea::vertical()
                        .id_salt("bulk_list_unmatched_scroll")
                        .max_height(80.0)
                        .show(ui, |ui| {
                            for name in &unmatched_snapshot {
                                ui.label(format!("• {name}"));
                            }
                        });
                }

                if !matches_snapshot.is_empty() {
                    ui.add_space(6.0);
                    // Tên khớp 1 THƯ MỤC thì cả thư mục (đệ quy) được tải —
                    // nên đếm là "mục", và ghi chú riêng số thư mục chưa
                    // biết dung lượng (giống nút "Tải N mục đã chọn").
                    let (known_bytes, unknown_folders) =
                        sum_known_size(&matches_snapshot, &folder_sizes);
                    let mut download_label = format!(
                        "⬇ Tải {} mục (~{})",
                        matches_snapshot.len(),
                        format_bytes(known_bytes)
                    );
                    if unknown_folders > 0 {
                        download_label
                            .push_str(&format!(" +{unknown_folders} thư mục chưa rõ dung lượng"));
                    }
                    if ui
                        .add_enabled(!busy, egui::Button::new(download_label))
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
                            format!("🗑 Chuyển {} file vào Thùng rác", matches_snapshot.len());
                        if ui.add_enabled(!busy, egui::Button::new(label)).clicked() {
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

    fn draw_job_status(&mut self, ui: &mut egui::Ui) {
        match &self.job {
            JobState::Idle => {}
            JobState::Loading => {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("Đang tải danh sách...");
                });
            }
            JobState::Scanning { found } => {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label(format!("Đang quét thư mục... đã tìm thấy {found} file"));
                    if ui.button("Hủy").clicked() {
                        self.cancel_flag.store(true, Ordering::Relaxed);
                    }
                });
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
                        .text(format!("{done_files}/{total_files} file")),
                );
                ui.horizontal(|ui| {
                    ui.label(format!(
                        "{} / {}",
                        format_bytes(*done_bytes),
                        format_bytes(*total_bytes)
                    ));
                    ui.weak(format!("• {}", format_speed(*speed_bps)));
                    let remaining_bytes = total_bytes.saturating_sub(*done_bytes);
                    if *speed_bps >= 1024.0 && remaining_bytes > 0 {
                        let eta_secs = remaining_bytes as f64 / *speed_bps;
                        ui.weak(format!("• còn khoảng {}", format_duration(eta_secs)));
                    }
                });
                if let Some(name) = last_started_name {
                    ui.weak(format!("Đang tải: {name}"));
                }
                if ui.button("Hủy").clicked() {
                    self.cancel_flag.store(true, Ordering::Relaxed);
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
                        .text(format!("Đang chuyển vào Thùng rác: {done}/{total}")),
                );
            }
        }

        if !self.log.is_empty() {
            egui::CollapsingHeader::new(format!("Nhật ký ({} dòng)", self.log.len()))
                .default_open(true)
                .show(ui, |ui| {
                    egui::ScrollArea::vertical()
                        .max_height(160.0)
                        .stick_to_bottom(true)
                        .show(ui, |ui| {
                            for line in &self.log {
                                ui.monospace(line);
                            }
                        });
                });
        }
    }
}

impl eframe::App for GDriveCopierApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.drain_events();
        self.update_speed_estimate();

        egui::CentralPanel::default().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Sao chép thư mục Google Drive công khai");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    self.draw_account_status(ui);
                });
            });
            ui.add_space(4.0);

            if self.show_settings || self.config.api_key.is_none() {
                self.draw_settings(ui);
                return;
            }

            self.draw_link_bar(ui);
            self.draw_breadcrumbs(ui);
            ui.separator();

            // Mọi thứ CÓ THỂ dài (danh sách file, xóa hàng loạt, banner xác
            // nhận, tiến độ, nhật ký...) đặt trong 1 vùng CUỘN ĐƯỢC. Đo
            // thẳng chiều cao CÒN LẠI của cửa sổ ngay tại đây rồi truyền
            // showo(`max_height`) — KHÔNG để `ScrollArea` tự suy luận —
            // vì đây là nguyên nhân trước đó khiến vùng cuộn cứ giãn to
            // theo đúng chiều cao nội dung thay vì co lại theo cửa sổ,
            // dẫn tới các nút ở cuối (đặc biệt banner xác nhận xóa, nút
            // "Tìm & xem trước") bị đẩy ra ngoài mà không cuộn tới được.
            let remaining_height = ui.available_height();
            egui::ScrollArea::vertical()
                .max_height(remaining_height)
                .show(ui, |ui| {
                    self.draw_entry_list(ui);
                    // Tải theo danh sách không cần đăng nhập nên chỉ cần đã mở
                    // 1 thư mục (có mục để khớp); riêng phần xóa cần đăng nhập
                    // (xem bên trong `draw_bulk_list_section`).
                    if self.is_logged_in() || !self.current_entries.is_empty() {
                        ui.separator();
                        self.draw_bulk_list_section(ui);
                    }
                    ui.separator();
                    self.draw_job_status(ui);

                    if let Some((msg, is_error)) = self.status_message.clone() {
                        ui.add_space(4.0);
                        let color = if is_error {
                            egui::Color32::from_rgb(224, 96, 96)
                        } else {
                            egui::Color32::from_rgb(96, 176, 112)
                        };
                        ui.colored_label(color, msg);
                    }
                });
        });

        if self.is_busy() {
            ui.ctx()
                .request_repaint_after(std::time::Duration::from_millis(80));
        }
    }
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
fn sum_known_size(
    entries: &[DriveEntry],
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
