//! Client gọi Google Drive API v3 chỉ bằng API key (KHÔNG OAuth, KHÔNG cần
//! người dùng đăng nhập). Chỉ hoạt động với các file/thư mục đã được chia sẻ
//! công khai ("Anyone with the link" / "Public on the web"), vì đó là điều
//! kiện để Google cho phép truy cập chỉ bằng API key.
//!
//! Tham khảo:
//! - https://developers.google.com/workspace/drive/api/guides/fields-parameter
//! - https://developers.google.com/workspace/drive/api/guides/manage-downloads

use crate::config::ConflictPolicy;
use futures_util::StreamExt;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

pub const API_BASE: &str = "https://www.googleapis.com/drive/v3";
pub const FOLDER_MIME: &str = "application/vnd.google-apps.folder";
const SHORTCUT_MIME: &str = "application/vnd.google-apps.shortcut";
const GOOGLE_APPS_PREFIX: &str = "application/vnd.google-apps.";

/// Một file hoặc thư mục trong Drive, đã được rút gọn về những gì app cần.
#[derive(Debug, Clone)]
pub struct DriveEntry {
    pub id: String,
    pub name: String,
    pub mime_type: String,
    /// `None` với thư mục, hoặc với file Google Docs/Sheets/Slides (Google
    /// không cho biết trước dung lượng xuất ra sẽ là bao nhiêu).
    pub size: Option<u64>,
    pub is_folder: bool,
}

#[derive(Debug, Deserialize)]
struct RawFile {
    id: String,
    name: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    // Quan trọng: Drive API v3 trả "size" dạng CHUỖI (vì là int64, tránh mất
    // độ chính xác khi số quá lớn cho JS/JSON number), nên phải nhận String
    // rồi tự parse sang u64.
    size: Option<String>,
    #[serde(rename = "shortcutDetails")]
    shortcut_details: Option<ShortcutDetails>,
}

#[derive(Debug, Deserialize)]
struct ShortcutDetails {
    #[serde(rename = "targetId")]
    target_id: Option<String>,
    #[serde(rename = "targetMimeType")]
    target_mime_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FileListResponse {
    #[serde(default)]
    files: Vec<RawFile>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiErrorEnvelope {
    error: ApiErrorBody,
}

#[derive(Debug, Deserialize)]
struct ApiErrorBody {
    #[allow(dead_code)]
    code: i64,
    message: String,
    #[serde(default)]
    errors: Vec<ApiErrorItem>,
}

/// Mã lỗi ngắn gọn Google gửi kèm bên trong `error.errors[]` (vd
/// `insufficientFilePermissions`, `rateLimitExceeded`...) — chi tiết hơn
/// nhiều so với `error.message` (chỉ là câu mô tả chung chung), dùng để
/// biết CHÍNH XÁC vì sao 1 request thất bại thay vì đoán qua mã HTTP.
#[derive(Debug, Deserialize, Default)]
struct ApiErrorItem {
    #[serde(default)]
    reason: String,
}

/// Đọc JSON lỗi Google trả về, tách riêng câu mô tả (`message`) và mã lỗi
/// ngắn gọn (`reason`) — trả `(None, None)` nếu body không phải JSON hợp lệ.
fn parse_api_error(body: &str) -> (Option<String>, Option<String>) {
    match serde_json::from_str::<ApiErrorEnvelope>(body) {
        Ok(e) => {
            let reason = e
                .error
                .errors
                .first()
                .map(|i| i.reason.clone())
                .filter(|r| !r.is_empty());
            (Some(e.error.message), reason)
        }
        Err(_) => (None, None),
    }
}

/// Diễn giải các lý do 403 hay gặp thành thông báo tiếng Việt kèm hướng khắc
/// phục cụ thể — trả `None` nếu không nhận diện được (Google có thể thêm lý
/// do mới), để bên gọi tự dùng thông báo chung chung (mã HTTP + message gốc).
///
/// QUAN TRỌNG: các lý do này đều là GIỚI HẠN CỐ ĐỊNH (chính sách chia sẻ do
/// chủ sở hữu đặt, hoặc quét mã độc của Google), không phải tình trạng tạm
/// thời như hết hạn mức — thử lại bao nhiêu lần cũng ra kết quả y hệt, nên
/// những lý do này KHÔNG được coi là "retryable" (xem `classify_http_error`).
fn friendly_permission_message(reason: &str) -> Option<String> {
    match reason {
        "insufficientFilePermissions" | "insufficientPermissions" => Some(
            "Chủ sở hữu đã TẮT quyền tải xuống/sao chép cho người xem trên file/thư mục này \
             (mục \"Người xem và người nhận xét có thể xem tùy chọn tải xuống, in và sao \
             chép\" đang tắt trong phần chia sẻ nâng cao trên Google Drive) — dù link đã ở \
             chế độ \"Bất kỳ ai có đường liên kết\" thì mục này vẫn tắt/bật RIÊNG, không phụ \
             thuộc nhau. Dùng cách nào để tải cũng gặp lỗi y hệt (kể cả bằng trình duyệt), \
             không phải lỗi của app. Cách khắc phục: nhờ chủ sở hữu vào Chia sẻ → biểu tượng \
             bánh răng (Cài đặt) → bật lại tùy chọn trên, hoặc xin cấp quyền xem/chỉnh sửa \
             trực tiếp cho tài khoản Google của bạn."
                .to_string(),
        ),
        "cannotDownloadAbusiveFile" => Some(
            "Google tự động gắn cờ file này là có khả năng độc hại (thường gặp với file \
             .exe/.zip lạ), nên CHỈ chủ sở hữu file mới tải trực tiếp được — người xem công \
             khai nào cũng bị chặn y hệt, kể cả mở bằng trình duyệt. Đây là giới hạn từ \
             Google, không phải lỗi của app."
                .to_string(),
        ),
        _ => None,
    }
}

/// Kết quả phân loại 1 response lỗi: thông báo hiển thị + có nên thử lại
/// hay không.
struct HttpErrorInfo {
    message: String,
    retryable: bool,
}

/// Dựng thông báo lỗi + quyết định thử lại từ 1 response HTTP không thành
/// công của Drive API. `generic_hint`, nếu có, chỉ được thêm vào khi KHÔNG
/// nhận diện được lý do cụ thể (ví dụ 404 sai link) — không thêm vào thông
/// báo đã đủ cụ thể ở trên, tránh lặp ý/gây rối.
fn classify_http_error(
    status_code: u16,
    is_server_error: bool,
    body: &str,
    generic_hint: Option<&str>,
) -> HttpErrorInfo {
    let (raw_message, reason) = parse_api_error(body);
    if status_code == 403 {
        if let Some(friendly) = reason.as_deref().and_then(friendly_permission_message) {
            return HttpErrorInfo {
                message: friendly,
                retryable: false,
            };
        }
    }
    let mut message = match raw_message {
        Some(m) => format!("HTTP {status_code}: {m}"),
        None => format!("HTTP {status_code}"),
    };
    if let Some(hint) = generic_hint {
        message.push_str(". ");
        message.push_str(hint);
    }
    // 429 (hết hạn mức tạm thời) và lỗi 5xx (máy chủ) luôn nên thử lại; 403
    // KHÔNG khớp lý do cố định nào ở trên (ví dụ rate-limit cũng dùng chung
    // mã 403) cũng vẫn thử lại — chỉ dừng hẳn khi đã nhận diện rõ là giới
    // hạn cố định (đã return sớm ở nhánh trên).
    let retryable = status_code == 429 || is_server_error || status_code == 403;
    HttpErrorInfo { message, retryable }
}

/// Chuyển 1 `RawFile` từ JSON thành `DriveEntry`, tự động "đi xuyên" shortcut
/// để trỏ thẳng tới file/thư mục đích (người dùng không cần quan tâm shortcut).
fn raw_file_to_entry(f: RawFile) -> DriveEntry {
    if f.mime_type == SHORTCUT_MIME {
        if let Some(sd) = f.shortcut_details {
            if let (Some(target_id), Some(target_mime)) = (sd.target_id, sd.target_mime_type) {
                return DriveEntry {
                    is_folder: target_mime == FOLDER_MIME,
                    id: target_id,
                    name: f.name,
                    mime_type: target_mime,
                    size: None,
                };
            }
        }
    }
    let size = f.size.and_then(|s| s.parse::<u64>().ok());
    DriveEntry {
        is_folder: f.mime_type == FOLDER_MIME,
        id: f.id,
        name: f.name,
        mime_type: f.mime_type,
        size,
    }
}

fn export_mime_for(google_mime: &str) -> &'static str {
    match google_mime {
        "application/vnd.google-apps.document" => {
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        }
        "application/vnd.google-apps.spreadsheet" => {
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
        "application/vnd.google-apps.presentation" => {
            "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        }
        "application/vnd.google-apps.drawing" => "image/png",
        // Các loại Google-app khác (Forms, Apps Script, Jamboard, My Maps...)
        // không có định dạng "gốc" tương đương -> xuất về PDF cho an toàn.
        _ => "application/pdf",
    }
}

fn export_extension_for(google_mime: &str) -> &'static str {
    match google_mime {
        "application/vnd.google-apps.document" => "docx",
        "application/vnd.google-apps.spreadsheet" => "xlsx",
        "application/vnd.google-apps.presentation" => "pptx",
        "application/vnd.google-apps.drawing" => "png",
        _ => "pdf",
    }
}

/// Tên file khi lưu ra đĩa. Với file Google Docs/Sheets/Slides, Google không
/// tự thêm đuôi file, nên ta phải tự thêm (.docx/.xlsx/.pptx/...) để file mở
/// được đúng ứng dụng trên máy người dùng.
pub fn suggested_filename(entry: &DriveEntry) -> String {
    if entry.mime_type.starts_with(GOOGLE_APPS_PREFIX) && entry.mime_type != FOLDER_MIME {
        format!("{}.{}", entry.name, export_extension_for(&entry.mime_type))
    } else {
        entry.name.clone()
    }
}

fn looks_like_drive_id(s: &str) -> bool {
    s.len() >= 10
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Trích ID thư mục/tệp từ nhiều dạng link Google Drive phổ biến, hoặc nhận
/// thẳng một ID trần nếu người dùng dán ID thay vì link.
///
/// Hỗ trợ:
/// - `.../drive/folders/{id}`
/// - `.../drive/u/0/folders/{id}`
/// - `.../file/d/{id}/view`
/// - `.../open?id={id}` , `.../folderview?id={id}`
/// - Dán thẳng ID (không có `/` hay `?`)
pub fn extract_drive_id(input: &str) -> anyhow::Result<String> {
    let input = input.trim();
    if input.is_empty() {
        anyhow::bail!("Vui lòng dán một đường link (hoặc ID) thư mục Google Drive");
    }
    if looks_like_drive_id(input) {
        return Ok(input.to_string());
    }
    let url =
        url::Url::parse(input).map_err(|_| anyhow::anyhow!("Đây không phải một URL hợp lệ"))?;
    let segments: Vec<&str> = url.path_segments().map(|s| s.collect()).unwrap_or_default();
    for pair in segments.windows(2) {
        if pair[0] == "folders" || pair[0] == "d" {
            return Ok(pair[1].to_string());
        }
    }
    if let Some(id) = url
        .query_pairs()
        .find(|(k, _)| k == "id")
        .map(|(_, v)| v.to_string())
    {
        return Ok(id);
    }
    anyhow::bail!("Không tìm thấy ID thư mục/tệp hợp lệ trong đường link này")
}

pub struct DriveClient {
    http: reqwest::Client,
    api_key: String,
}

/// Kết quả của 1 lần gọi `download_file` — phân biệt "đã thật sự tải" với
/// "bỏ qua vì đã có file" để phía trên (log, thống kê) hiển thị đúng.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadOutcome {
    Downloaded,
    Skipped,
}

/// Lỗi của 1 LẦN THỬ tải (`download_attempt`) — phân biệt có đáng thử lại
/// hay không. Trước đây MỌI lỗi đều được `download_file` thử lại mù quáng
/// tới `MAX_ATTEMPTS` lần (có thể mất hơn 1 phút với backoff tăng dần) rồi
/// mới báo lỗi, kể cả khi lỗi là giới hạn cố định (ví dụ chủ sở hữu tắt
/// quyền tải xuống) mà chắc chắn thử lại bao nhiêu cũng không thể thành
/// công — vừa mất thời gian vô ích, vừa không giải thích được lý do thật.
enum AttemptError {
    /// Có khả năng là tạm thời (mất mạng, hết hạn mức, lỗi máy chủ...).
    Retriable(anyhow::Error),
    /// Giới hạn cố định, biết chắc thử lại cũng vô ích.
    Permanent(anyhow::Error),
}

impl From<std::io::Error> for AttemptError {
    fn from(e: std::io::Error) -> Self {
        AttemptError::Retriable(e.into())
    }
}

impl From<anyhow::Error> for AttemptError {
    fn from(e: anyhow::Error) -> Self {
        AttemptError::Retriable(e)
    }
}

impl DriveClient {
    pub fn new(api_key: String) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent("gdrive-copier/0.1 (+https://developers.google.com/workspace/drive)")
            .timeout(Duration::from_secs(120))
            .build()?;
        Ok(Self { http, api_key })
    }

    /// GET có retry/backoff cho lỗi tạm thời (403 rate limit, 429, 5xx) —
    /// KHÔNG thử lại nếu lỗi 403 là giới hạn cố định đã nhận diện được (xem
    /// `classify_http_error`), vì thử bao nhiêu lần cũng vô ích.
    async fn get_with_retry(&self, url: reqwest::Url) -> anyhow::Result<reqwest::Response> {
        const MAX_ATTEMPTS: u32 = 5;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match self.http.get(url.clone()).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return Ok(resp);
                    }
                    let status_code = status.as_u16();
                    let is_server_error = status.is_server_error();
                    let body = resp.text().await.unwrap_or_default();
                    let info = classify_http_error(
                        status_code,
                        is_server_error,
                        &body,
                        Some(
                            "Hãy kiểm tra: (1) link đã đúng, (2) đã chia sẻ ở chế độ \"Bất kỳ \
                             ai có đường liên kết\", (3) API key hợp lệ.",
                        ),
                    );
                    if info.retryable && attempt < MAX_ATTEMPTS {
                        let backoff_ms = 500u64 * (1u64 << (attempt - 1).min(4));
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    anyhow::bail!(info.message);
                }
                Err(e) => {
                    if attempt < MAX_ATTEMPTS {
                        let backoff_ms = 500u64 * (1u64 << (attempt - 1).min(4));
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    return Err(anyhow::anyhow!("Lỗi kết nối mạng: {e}"));
                }
            }
        }
    }

    /// Liệt kê TOÀN BỘ file/thư mục con trực tiếp bên trong 1 thư mục (tự
    /// động phân trang qua `nextPageToken`, nên không giới hạn ở 1000 mục).
    pub async fn list_folder_children(&self, folder_id: &str) -> anyhow::Result<Vec<DriveEntry>> {
        let mut out = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut url = reqwest::Url::parse(&format!("{API_BASE}/files"))?;
            {
                let mut qp = url.query_pairs_mut();
                qp.append_pair("q", &format!("'{folder_id}' in parents and trashed = false"));
                qp.append_pair("key", &self.api_key);
                qp.append_pair(
                    "fields",
                    "nextPageToken, files(id, name, mimeType, size, shortcutDetails)",
                );
                qp.append_pair("pageSize", "1000");
                qp.append_pair("supportsAllDrives", "true");
                qp.append_pair("includeItemsFromAllDrives", "true");
                qp.append_pair("orderBy", "folder,name_natural");
                if let Some(pt) = &page_token {
                    qp.append_pair("pageToken", pt);
                }
            }
            let resp = self.get_with_retry(url).await?;
            let text = resp.text().await?;
            let parsed: FileListResponse = serde_json::from_str(&text)
                .map_err(|e| anyhow::anyhow!("Không đọc được phản hồi từ Google Drive: {e}"))?;
            for f in parsed.files {
                out.push(raw_file_to_entry(f));
            }
            match parsed.next_page_token {
                Some(t) => page_token = Some(t),
                None => break,
            }
        }
        Ok(out)
    }

    /// Lấy metadata của chính 1 ID (dùng để xác nhận link hợp lệ + lấy tên
    /// thư mục gốc hiển thị lên tiêu đề).
    pub async fn get_metadata(&self, file_id: &str) -> anyhow::Result<DriveEntry> {
        let mut url = reqwest::Url::parse(&format!("{API_BASE}/files/{file_id}"))?;
        url.query_pairs_mut()
            .append_pair("key", &self.api_key)
            .append_pair("fields", "id,name,mimeType,size,shortcutDetails")
            .append_pair("supportsAllDrives", "true");
        let resp = self.get_with_retry(url).await?;
        let text = resp.text().await?;
        let raw: RawFile = serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!("Không đọc được metadata: {e}"))?;
        Ok(raw_file_to_entry(raw))
    }

    fn build_download_url(&self, entry: &DriveEntry) -> anyhow::Result<reqwest::Url> {
        if entry.mime_type.starts_with(GOOGLE_APPS_PREFIX) {
            let export_mime = export_mime_for(&entry.mime_type);
            let mut url = reqwest::Url::parse(&format!("{API_BASE}/files/{}/export", entry.id))?;
            url.query_pairs_mut()
                .append_pair("mimeType", export_mime)
                .append_pair("key", &self.api_key);
            Ok(url)
        } else {
            let mut url = reqwest::Url::parse(&format!("{API_BASE}/files/{}", entry.id))?;
            url.query_pairs_mut()
                .append_pair("alt", "media")
                .append_pair("key", &self.api_key)
                .append_pair("supportsAllDrives", "true");
            Ok(url)
        }
    }

    /// Tải 1 file về `dest_path`, ghi trực tiếp xuống đĩa theo dạng stream
    /// (không load hết vào RAM, quan trọng với file video vài GB).
    ///
    /// `conflict_policy` quyết định phải làm gì nếu `dest_path` đã tồn tại
    /// sẵn (từ lần chạy trước, hoặc người dùng đã có file cùng tên):
    /// bỏ qua / ghi đè / lưu bản mới với tên `_v1`, `_v2`...
    ///
    /// Nếu mất kết nối giữa chừng (rất hay gặp với file vài trăm MB — vài
    /// GB), tự động thử lại tối đa `MAX_ATTEMPTS` lần, mỗi lần tải TIẾP từ
    /// đúng chỗ đã dừng (qua HTTP header `Range`) thay vì tải lại từ đầu —
    /// kể cả khi app bị đóng và mở lại sau đó, vì vị trí resume được đọc
    /// trực tiếp từ dung lượng file `.part` đang có trên đĩa. Nếu vì lý do
    /// nào đó máy chủ không tôn trọng `Range` (trả về 200 thay vì 206), tự
    /// nhận ra và tải lại từ đầu thay vì ghép nối nhầm dữ liệu.
    ///
    /// `on_retry(lần_thử, tổng_số_lần_cho_phép, thông_báo_lỗi)` được gọi mỗi
    /// khi 1 lần thử thất bại (kể cả lần cuối cùng) — dùng để hiện chi tiết
    /// từng lần thử lên nhật ký, giúp chẩn đoán các lỗi kiểu hạn mức/rate
    /// limit vốn khó thấy rõ nguyên nhân chỉ qua 1 lần thử.
    ///
    /// Kiểm tra `cancel` định kỳ trong lúc stream để hủy giữa chừng được.
    pub async fn download_file(
        &self,
        entry: &DriveEntry,
        dest_path: &Path,
        conflict_policy: ConflictPolicy,
        mut on_progress: impl FnMut(u64, Option<u64>),
        mut on_retry: impl FnMut(u32, u32, &str),
        cancel: &Arc<AtomicBool>,
    ) -> anyhow::Result<DownloadOutcome> {
        if let Some(parent) = dest_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let final_path = match conflict_policy {
            ConflictPolicy::Overwrite => dest_path.to_path_buf(),
            ConflictPolicy::Skip => {
                if let Ok(meta) = tokio::fs::metadata(dest_path).await {
                    if meta.is_file() {
                        let size = entry.size.unwrap_or(meta.len());
                        on_progress(size, Some(size));
                        return Ok(DownloadOutcome::Skipped);
                    }
                }
                dest_path.to_path_buf()
            }
            ConflictPolicy::Rename => find_available_renamed_path(dest_path).await,
        };

        let mut tmp_name = final_path.as_os_str().to_owned();
        tmp_name.push(".part");
        let tmp_path = PathBuf::from(tmp_name);

        // File xuất từ Google Docs/Sheets/Slides (export) không hỗ trợ tải
        // tiếp theo Range -> luôn bỏ phần dở dang, tải lại từ đầu cho chắc.
        let can_resume = !entry.mime_type.starts_with(GOOGLE_APPS_PREFIX);
        if !can_resume {
            let _ = tokio::fs::remove_file(&tmp_path).await;
        }

        const MAX_ATTEMPTS: u32 = 6;
        let mut downloaded: u64 = tokio::fs::metadata(&tmp_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        if downloaded > 0 {
            on_progress(downloaded, entry.size);
        }

        for attempt in 1..=MAX_ATTEMPTS {
            let resume_from = if can_resume { downloaded } else { 0 };
            match self
                .download_attempt(entry, &tmp_path, resume_from, &mut on_progress, cancel)
                .await
            {
                Ok(()) => {
                    tokio::fs::rename(&tmp_path, &final_path).await?;
                    return Ok(DownloadOutcome::Downloaded);
                }
                Err(AttemptError::Permanent(e)) => {
                    // Giới hạn cố định (vd chủ sở hữu tắt quyền tải xuống) —
                    // thử lại chắc chắn vô ích, dừng ngay thay vì chờ hết
                    // MAX_ATTEMPTS lần mới báo cùng 1 lỗi.
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                    return Err(e);
                }
                Err(AttemptError::Retriable(e)) => {
                    if cancel.load(Ordering::Relaxed) {
                        let _ = tokio::fs::remove_file(&tmp_path).await;
                        anyhow::bail!("Đã hủy");
                    }
                    // Báo cho bên gọi biết CHI TIẾT lỗi của TỪNG lần thử
                    // (không chỉ lần cuối) để còn hiện lên nhật ký — quan
                    // trọng để chẩn đoán những lỗi kiểu hạn mức/rate-limit
                    // vốn không rõ nguyên nhân ngay từ 1 lần thử duy nhất.
                    on_retry(attempt, MAX_ATTEMPTS, &e.to_string());
                    if attempt == MAX_ATTEMPTS {
                        return Err(anyhow::anyhow!(
                            "Đã thử lại {MAX_ATTEMPTS} lần vẫn lỗi. Lỗi gần nhất: {e}"
                        ));
                    }
                    // Đọc lại dung lượng THẬT SỰ đã ghi được (kể cả khi máy
                    // chủ không tôn trọng Range và file đã bị ghi lại từ đầu
                    // ở lần thử vừa rồi) để lần sau resume đúng chỗ.
                    downloaded = tokio::fs::metadata(&tmp_path)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);
                }
            }
            let backoff_secs = 2u64.saturating_pow(attempt.min(5));
            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
        }
        anyhow::bail!("Đã thử lại {MAX_ATTEMPTS} lần vẫn lỗi khi tải file này")
    }

    /// 1 lần thử tải (từ đầu hoặc tiếp từ `resume_from`), ghi thẳng vào
    /// `tmp_path`. Trả lỗi nếu mất kết nối/HTTP lỗi giữa chừng — bên gọi
    /// (`download_file`) sẽ đọc lại dung lượng đã ghi và thử lại.
    async fn download_attempt(
        &self,
        entry: &DriveEntry,
        tmp_path: &Path,
        resume_from: u64,
        on_progress: &mut impl FnMut(u64, Option<u64>),
        cancel: &Arc<AtomicBool>,
    ) -> Result<(), AttemptError> {
        let url = self.build_download_url(entry)?;
        let mut req = self.http.get(url);
        if resume_from > 0 {
            req = req.header("Range", format!("bytes={resume_from}-"));
        }
        let resp = req
            .send()
            .await
            .map_err(|e| AttemptError::Retriable(anyhow::anyhow!("Lỗi kết nối mạng: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let status_code = status.as_u16();
            let is_server_error = status.is_server_error();
            let body = resp.text().await.unwrap_or_default();
            let info = classify_http_error(status_code, is_server_error, &body, None);
            return Err(if info.retryable {
                AttemptError::Retriable(anyhow::anyhow!(info.message))
            } else {
                AttemptError::Permanent(anyhow::anyhow!(info.message))
            });
        }

        // Chỉ coi là ĐANG resume nếu máy chủ THỰC SỰ trả 206 (tôn trọng
        // Range) — nếu ta xin tiếp từ giữa mà máy chủ vẫn trả 200 (bỏ qua
        // Range), phải ghi lại từ đầu, không được nối vào file cũ (sẽ hỏng).
        let effectively_resumed = status.as_u16() == 206 && resume_from > 0;
        let total = resp
            .content_length()
            .map(|len| if effectively_resumed { len + resume_from } else { len })
            .or(entry.size);

        let mut file = if effectively_resumed {
            tokio::fs::OpenOptions::new()
                .append(true)
                .open(tmp_path)
                .await?
        } else {
            tokio::fs::File::create(tmp_path).await?
        };

        let mut downloaded = if effectively_resumed { resume_from } else { 0 };
        on_progress(downloaded, total);
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            if cancel.load(Ordering::Relaxed) {
                return Err(AttemptError::Retriable(anyhow::anyhow!("Đã hủy")));
            }
            let chunk = chunk.map_err(|e| anyhow::anyhow!("Mất kết nối khi đang tải: {e}"))?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            on_progress(downloaded, total);
        }
        file.flush().await?;
        Ok(())
    }
}

/// Tìm 1 đường dẫn còn trống bằng cách thêm hậu tố `_v1`, `_v2`... vào TÊN
/// (giữ nguyên đuôi file), dùng cho `ConflictPolicy::Rename`. Nếu `path`
/// chưa tồn tại thì trả về luôn chính nó.
async fn find_available_renamed_path(path: &Path) -> PathBuf {
    if tokio::fs::metadata(path).await.is_err() {
        return path.to_path_buf();
    }
    let parent = path.parent().map(|p| p.to_path_buf()).unwrap_or_default();
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let ext = path.extension().map(|s| s.to_string_lossy().to_string());
    let mut n = 1u32;
    loop {
        let candidate_name = match &ext {
            Some(e) => format!("{stem}_v{n}.{e}"),
            None => format!("{stem}_v{n}"),
        };
        let candidate = parent.join(candidate_name);
        if tokio::fs::metadata(&candidate).await.is_err() {
            return candidate;
        }
        n += 1;
    }
}

/// Thông tin chủ sở hữu + thư mục cha hiện tại của 1 file — kết quả của
/// `DriveEditClient::check_ownership`.
#[derive(Debug, Clone)]
pub struct OwnershipInfo {
    pub owned_by_me: bool,
    /// Tên (ưu tiên) hoặc email của chủ sở hữu — `None` khi `owned_by_me`
    /// là `true` (không cần hiện chủ sở hữu là chính mình).
    pub owner_label: Option<String>,
    pub parent_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawOwnership {
    #[serde(default)]
    owners: Vec<RawOwner>,
    #[serde(default)]
    parents: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawOwner {
    #[serde(rename = "displayName")]
    display_name: Option<String>,
    #[serde(rename = "emailAddress")]
    email_address: Option<String>,
    #[serde(default)]
    me: Option<bool>,
}

/// Client cho các thao tác GHI (đổi tên, xóa) — dùng Bearer token từ đăng
/// nhập Google (OAuth), KHÔNG dùng API key. Đây là 2 client HOÀN TOÀN tách
/// biệt: `DriveClient` (đọc, API key, không cần đăng nhập) vẫn hoạt động y
/// nguyên dù người dùng có đăng nhập hay không.
pub struct DriveEditClient {
    http: reqwest::Client,
}

impl DriveEditClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }

    /// Đổi tên 1 file/thư mục. Cần tài khoản đăng nhập có quyền edit trên
    /// file đó (chủ sở hữu, hoặc được chia sẻ ở chế độ "Editor").
    pub async fn rename_file(
        &self,
        access_token: &str,
        file_id: &str,
        new_name: &str,
    ) -> anyhow::Result<()> {
        let url = format!("{API_BASE}/files/{file_id}");
        let body = serde_json::json!({ "name": new_name });
        let resp = self
            .http
            .patch(&url)
            .bearer_auth(access_token)
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Lỗi kết nối mạng: {e}"))?;
        check_write_response(resp, WriteOperation::Rename).await
    }

    /// Chuyển 1 file/thư mục vào Thùng rác (KHÔNG xóa vĩnh viễn) — an toàn
    /// hơn xóa hẳn, người dùng có thể khôi phục lại trong vòng 30 ngày từ
    /// chính giao diện Google Drive nếu lỡ tay.
    pub async fn trash_file(&self, access_token: &str, file_id: &str) -> anyhow::Result<()> {
        let url = format!("{API_BASE}/files/{file_id}");
        let body = serde_json::json!({ "trashed": true });
        let resp = self
            .http
            .patch(&url)
            .bearer_auth(access_token)
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Lỗi kết nối mạng: {e}"))?;
        check_write_response(resp, WriteOperation::Trash).await
    }

    /// Thông tin chủ sở hữu + thư mục cha hiện tại của 1 file. Bắt buộc gọi
    /// bằng Bearer token (không phải API key) vì trường `me` (đang đăng
    /// nhập có phải chủ sở hữu không) chỉ có ý nghĩa khi đã biết đang GỌI
    /// NHÂN DANH AI — dùng để biết TRƯỚC khi thử xóa xem tài khoản đang
    /// đăng nhập có chuyển thẳng vào Thùng rác được không (chỉ chủ sở hữu
    /// mới được, xem `WriteOperation::Trash`), thay vì để người dùng gặp
    /// lỗi 403 khó hiểu rồi mới biết.
    pub async fn check_ownership(
        &self,
        access_token: &str,
        file_id: &str,
    ) -> anyhow::Result<OwnershipInfo> {
        let url = format!("{API_BASE}/files/{file_id}");
        let resp = self
            .http
            .get(&url)
            .bearer_auth(access_token)
            .query(&[("fields", "owners(displayName,emailAddress,me),parents")])
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Lỗi kết nối mạng: {e}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            let status_code = status.as_u16();
            let (raw_message, _reason) = parse_api_error(&text);
            let message = match raw_message {
                Some(m) => format!("HTTP {status_code}: {m}"),
                None => format!("HTTP {status_code}"),
            };
            anyhow::bail!("Không kiểm tra được chủ sở hữu: {message}");
        }
        let raw: RawOwnership = serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!("Không đọc được thông tin chủ sở hữu: {e}"))?;
        let owned_by_me = raw.owners.iter().any(|o| o.me.unwrap_or(false));
        // Không phải chủ sở hữu thì mới cần biết chủ sở hữu là ai để hiện
        // lên giao diện — ưu tiên TÊN cho thân thiện, thiếu thì dùng email.
        let owner_label = if owned_by_me {
            None
        } else {
            raw.owners
                .first()
                .and_then(|o| o.display_name.clone().or_else(|| o.email_address.clone()))
        };
        Ok(OwnershipInfo {
            owned_by_me,
            owner_label,
            parent_id: raw.parents.into_iter().next(),
        })
    }

    /// Tìm thư mục con tên `folder_name` bên trong `parent_id`; nếu chưa có
    /// thì tạo mới. Gọi lại nhiều lần chỉ tạo ra ĐÚNG 1 thư mục (tìm trước,
    /// tạo sau) — dùng làm nơi "gom" các file không do mình sở hữu khi xóa
    /// hàng loạt, để không tạo trùng thư mục mỗi lần bấm xóa.
    pub async fn find_or_create_folder(
        &self,
        access_token: &str,
        parent_id: &str,
        folder_name: &str,
    ) -> anyhow::Result<String> {
        #[derive(Debug, Deserialize)]
        struct FileIdOnly {
            id: String,
        }
        #[derive(Debug, Deserialize)]
        struct SearchResult {
            #[serde(default)]
            files: Vec<FileIdOnly>,
        }

        let escaped_name = folder_name.replace('\\', "\\\\").replace('\'', "\\'");
        let query = format!(
            "name = '{escaped_name}' and '{parent_id}' in parents and mimeType = \
             '{FOLDER_MIME}' and trashed = false"
        );
        let search_url = reqwest::Url::parse(&format!("{API_BASE}/files"))?;
        let resp = self
            .http
            .get(search_url)
            .bearer_auth(access_token)
            .query(&[("q", query.as_str()), ("fields", "files(id)")])
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Lỗi kết nối mạng: {e}"))?;
        if resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            if let Ok(found) = serde_json::from_str::<SearchResult>(&text) {
                if let Some(existing) = found.files.into_iter().next() {
                    return Ok(existing.id);
                }
            }
        }

        // Chưa có (hoặc tìm lỗi, cứ thử tạo — nếu do trùng tên thật thì lần
        // tìm sau sẽ thấy) — tạo mới.
        #[derive(Debug, Deserialize)]
        struct CreatedFile {
            id: String,
        }
        let create_body = serde_json::json!({
            "name": folder_name,
            "mimeType": FOLDER_MIME,
            "parents": [parent_id],
        });
        let resp = self
            .http
            .post(format!("{API_BASE}/files"))
            .bearer_auth(access_token)
            .json(&create_body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Lỗi kết nối mạng: {e}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            let status_code = status.as_u16();
            let (raw_message, _reason) = parse_api_error(&text);
            let message = match raw_message {
                Some(m) => format!("HTTP {status_code}: {m}"),
                None => format!("HTTP {status_code}"),
            };
            anyhow::bail!("Không tạo được thư mục \"{folder_name}\": {message}");
        }
        let created: CreatedFile = serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!("Không đọc được phản hồi khi tạo thư mục: {e}"))?;
        Ok(created.id)
    }

    /// Chuyển 1 file sang thư mục khác (đổi `parents`), tùy chọn đổi tên
    /// cùng lúc. Dùng để "xóa" các file KHÔNG do tài khoản đang đăng nhập
    /// sở hữu: vì Google chỉ cho phép CHỦ SỞ HỮU chuyển thẳng vào Thùng rác
    /// (xem `WriteOperation::Trash`), phương án thay thế là chuyển file
    /// sang 1 thư mục riêng để chủ sở hữu tự dọn sau — CHUYỂN THƯ MỤC là
    /// thao tác Editor bình thường vẫn làm được, không đòi hỏi là chủ sở
    /// hữu.
    pub async fn move_file(
        &self,
        access_token: &str,
        file_id: &str,
        from_parent_id: &str,
        to_parent_id: &str,
        rename_to: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut url = reqwest::Url::parse(&format!("{API_BASE}/files/{file_id}"))?;
        url.query_pairs_mut()
            .append_pair("addParents", to_parent_id)
            .append_pair("removeParents", from_parent_id);
        let mut req = self.http.patch(url).bearer_auth(access_token);
        if let Some(name) = rename_to {
            req = req.json(&serde_json::json!({ "name": name }));
        }
        let resp = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Lỗi kết nối mạng: {e}"))?;
        check_write_response(resp, WriteOperation::Move).await
    }
}

/// Thao tác GHI nào đang được kiểm tra kết quả — CÙNG một mã lỗi
/// `insufficientFilePermissions` của Google mang Ý NGHĨA KHÁC HẲN tùy thao
/// tác: theo tài liệu chính thức của Google
/// (developers.google.com/workspace/drive/api/guides/delete), CHUYỂN VÀO
/// THÙNG RÁC chỉ CHỦ SỞ HỮU mới làm được — quyền "Editor" (kể cả qua "Bất
/// kỳ ai có đường liên kết: Người chỉnh sửa") KHÔNG đủ, đây là quy định
/// cứng của Google áp dụng cho mọi ứng dụng, không phải lỗi cấu hình chia
/// sẻ. Trong khi đó ĐỔI TÊN là thao tác sửa nội dung bình thường, bất kỳ ai
/// có quyền Editor đều làm được — 403 lúc đổi tên nghĩa là tài khoản đang
/// đăng nhập THỰC SỰ chưa có quyền Editor, khác hẳn nguyên nhân lúc xóa.
#[derive(Clone, Copy)]
enum WriteOperation {
    Rename,
    Trash,
    Move,
}

fn friendly_write_permission_message(op: WriteOperation, reason: &str) -> Option<String> {
    match (op, reason) {
        (WriteOperation::Trash, "insufficientFilePermissions" | "insufficientPermissions") => {
            Some(
                "Google CHỈ cho phép CHỦ SỞ HỮU file chuyển file đó vào thùng rác — đây là quy \
                 định cứng từ phía Google, áp dụng cho MỌI ứng dụng (kể cả giao diện Google \
                 Drive gốc), không liên quan tới việc file đã mở quyền \"Bất kỳ ai có đường \
                 liên kết: Người chỉnh sửa\" hay chưa. Quyền Editor cho phép sửa nội dung, đổi \
                 tên... nhưng KHÔNG cho xóa nếu bạn không phải chủ sở hữu. Cách duy nhất: đăng \
                 nhập app bằng đúng tài khoản Google là CHỦ SỞ HỮU file, hoặc nhờ chủ sở hữu tự \
                 xóa giúp."
                    .to_string(),
            )
        }
        (WriteOperation::Rename, "insufficientFilePermissions" | "insufficientPermissions") => {
            Some(
                "Tài khoản Google đang đăng nhập trong app chưa có quyền Chỉnh sửa (Editor) \
                 trên file/thư mục này. Hãy kiểm tra: (1) link đã mở đúng chế độ \"Bất kỳ ai \
                 có đường liên kết: Người chỉnh sửa\" (không phải \"Người xem\"), (2) đang đăng \
                 nhập app bằng đúng tài khoản Google dự định dùng."
                    .to_string(),
            )
        }
        (WriteOperation::Move, "insufficientFilePermissions" | "insufficientPermissions") => Some(
            "Tài khoản đang đăng nhập không có đủ quyền chỉnh sửa trên thư mục gốc hoặc thư \
             mục đích để chuyển file này sang chỗ khác."
                .to_string(),
        ),
        (_, "cannotDownloadAbusiveFile") => Some(
            "Google tự động gắn cờ file này là có khả năng độc hại, hạn chế thao tác trên file \
             này — chỉ chủ sở hữu mới thao tác được. Đây là giới hạn từ Google, không phải lỗi \
             của app."
                .to_string(),
        ),
        _ => None,
    }
}

async fn check_write_response(resp: reqwest::Response, op: WriteOperation) -> anyhow::Result<()> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let status_code = status.as_u16();
    let body = resp.text().await.unwrap_or_default();
    let (raw_message, reason) = parse_api_error(&body);
    if status_code == 403 {
        if let Some(friendly) = reason
            .as_deref()
            .and_then(|r| friendly_write_permission_message(op, r))
        {
            anyhow::bail!(friendly);
        }
    }
    let message = match raw_message {
        Some(m) => format!("HTTP {status_code}: {m}"),
        None => format!("HTTP {status_code}"),
    };
    anyhow::bail!(message);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_common_drive_url_shapes() {
        let cases = [
            (
                "https://drive.google.com/drive/folders/1AbCdEfGhIjKlMnOp?usp=sharing",
                "1AbCdEfGhIjKlMnOp",
            ),
            (
                "https://drive.google.com/drive/u/0/folders/1AbCdEfGhIjKlMnOp",
                "1AbCdEfGhIjKlMnOp",
            ),
            (
                "https://drive.google.com/open?id=1AbCdEfGhIjKlMnOp",
                "1AbCdEfGhIjKlMnOp",
            ),
            (
                "https://drive.google.com/file/d/1AbCdEfGhIjKlMnOp/view?usp=sharing",
                "1AbCdEfGhIjKlMnOp",
            ),
            ("1AbCdEfGhIjKlMnOp", "1AbCdEfGhIjKlMnOp"),
        ];
        for (input, expected) in cases {
            assert_eq!(extract_drive_id(input).unwrap(), expected, "input: {input}");
        }
    }

    #[test]
    fn rejects_garbage_input() {
        assert!(extract_drive_id("").is_err());
        assert!(extract_drive_id("not a url at all but has spaces").is_err());
        assert!(extract_drive_id("short").is_err());
        assert!(extract_drive_id("https://example.com/nothing/here").is_err());
    }

    #[test]
    fn parses_realistic_file_list_with_large_size_and_shortcut() {
        let body = r#"
        {
          "nextPageToken": "abc123",
          "files": [
            { "id": "folder-1", "name": "Anh 2026", "mimeType": "application/vnd.google-apps.folder" },
            { "id": "file-1", "name": "video.mp4", "mimeType": "video/mp4", "size": "5368709120" },
            { "id": "shortcut-1", "name": "Loi tat", "mimeType": "application/vnd.google-apps.shortcut",
              "shortcutDetails": { "targetId": "real-file-42", "targetMimeType": "application/pdf" } }
          ]
        }
        "#;
        let parsed: FileListResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.next_page_token.as_deref(), Some("abc123"));
        let entries: Vec<DriveEntry> = parsed.files.into_iter().map(raw_file_to_entry).collect();
        assert_eq!(entries[1].size, Some(5_368_709_120u64));
        assert_eq!(entries[2].id, "real-file-42");
        assert!(!entries[2].is_folder);
    }

    #[test]
    fn suggested_filename_appends_export_extension_only_for_google_native_types() {
        let gdoc = DriveEntry {
            id: "x".into(),
            name: "Bao cao".into(),
            mime_type: "application/vnd.google-apps.document".into(),
            size: None,
            is_folder: false,
        };
        assert_eq!(suggested_filename(&gdoc), "Bao cao.docx");

        let normal = DriveEntry {
            id: "y".into(),
            name: "anh.jpg".into(),
            mime_type: "image/jpeg".into(),
            size: Some(1),
            is_folder: false,
        };
        assert_eq!(suggested_filename(&normal), "anh.jpg");
    }

    const INSUFFICIENT_PERMS_BODY: &str = r#"{
        "error": {
            "errors": [{"domain": "global", "reason": "insufficientFilePermissions",
                        "message": "The user does not have sufficient permissions for this file."}],
            "code": 403,
            "message": "The user does not have sufficient permissions for this file."
        }
    }"#;

    const RATE_LIMIT_BODY: &str = r#"{
        "error": {
            "errors": [{"domain": "usageLimits", "reason": "userRateLimitExceeded",
                        "message": "User Rate Limit Exceeded"}],
            "code": 403,
            "message": "User Rate Limit Exceeded"
        }
    }"#;

    #[test]
    fn extracts_reason_alongside_message_from_google_error_json() {
        let (message, reason) = parse_api_error(INSUFFICIENT_PERMS_BODY);
        assert_eq!(reason.as_deref(), Some("insufficientFilePermissions"));
        assert!(message.unwrap().contains("sufficient permissions"));
    }

    #[test]
    fn garbage_body_parses_to_no_message_no_reason() {
        let (message, reason) = parse_api_error("<html>not json</html>");
        assert_eq!(message, None);
        assert_eq!(reason, None);
    }

    /// Lỗi cốt lõi cần sửa: trước đây MỌI 403 đều bị coi là đáng thử lại,
    /// khiến 1 file bị chủ sở hữu tắt quyền tải xuống phải chờ hết
    /// MAX_ATTEMPTS lần (có thể hơn 1 phút) rồi mới báo lỗi mù mờ. Giờ phải
    /// dừng NGAY và có thông báo cụ thể.
    #[test]
    fn insufficient_permissions_403_stops_immediately_with_friendly_message() {
        let info = classify_http_error(403, false, INSUFFICIENT_PERMS_BODY, None);
        assert!(!info.retryable, "phải dừng ngay, không thử lại vô ích");
        assert!(info.message.contains("TẮT quyền tải xuống"));
    }

    #[test]
    fn rate_limit_403_still_gets_retried() {
        let info = classify_http_error(403, false, RATE_LIMIT_BODY, None);
        assert!(info.retryable, "rate limit là tạm thời, vẫn phải thử lại như trước");
    }

    #[test]
    fn unknown_403_reason_defaults_to_retryable() {
        let body = r#"{"error":{"code":403,"message":"Ly do moi Google them sau nay",
                                 "errors":[{"reason":"someBrandNewReason"}]}}"#;
        let info = classify_http_error(403, false, body, None);
        assert!(
            info.retryable,
            "lý do lạ chưa biết vẫn nên thử lại (an toàn hơn bỏ cuộc ngay khi chưa chắc)"
        );
    }

    #[test]
    fn server_error_5xx_still_retryable_without_403() {
        let info = classify_http_error(503, true, "", None);
        assert!(info.retryable);
        assert_eq!(info.message, "HTTP 503");
    }

    /// Cùng 1 mã lỗi `insufficientFilePermissions` của Google nhưng Ý NGHĨA
    /// khác hẳn tùy thao tác (theo tài liệu chính thức của Google): XÓA chỉ
    /// chủ sở hữu mới làm được (Editor không đủ), còn ĐỔI TÊN thì Editor
    /// bình thường vẫn làm được — 2 thông báo không được lẫn vào nhau.
    #[test]
    fn trash_context_blames_ownership_not_download_settings() {
        let msg =
            friendly_write_permission_message(WriteOperation::Trash, "insufficientFilePermissions")
                .unwrap();
        assert!(msg.contains("CHỦ SỞ HỮU"));
        assert!(
            !msg.contains("tải xuống"),
            "không được lẫn thông báo về quyền TẢI XUỐNG (khác ngữ cảnh) vào đây"
        );
    }

    #[test]
    fn rename_context_blames_editor_access_not_ownership() {
        let msg =
            friendly_write_permission_message(WriteOperation::Rename, "insufficientFilePermissions")
                .unwrap();
        assert!(msg.contains("Chỉnh sửa"));
        assert!(
            !msg.contains("CHỦ SỞ HỮU"),
            "đổi tên không đòi hỏi phải là chủ sở hữu, không nên nói vậy"
        );
    }

    #[test]
    fn same_google_reason_yields_different_message_per_write_operation() {
        let trash_msg =
            friendly_write_permission_message(WriteOperation::Trash, "insufficientFilePermissions");
        let rename_msg =
            friendly_write_permission_message(WriteOperation::Rename, "insufficientFilePermissions");
        assert_ne!(trash_msg, rename_msg);
    }
}
