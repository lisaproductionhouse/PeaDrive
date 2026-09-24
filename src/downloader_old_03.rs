//! Điều phối việc quét đệ quy 1 thư mục Drive và tải nhiều file song song
//! (giới hạn số luồng), báo tiến độ về GUI qua 1 kênh (channel) sự kiện.

use crate::config::ConflictPolicy;
use crate::drive_api::{suggested_filename, DownloadOutcome, DriveClient, DriveEntry};
use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

/// Các sự kiện gửi từ tác vụ nền (chạy trên tokio) về cho GUI.
#[derive(Debug, Clone)]
pub enum WorkerEvent {
    /// Đã lấy xong danh sách file/thư mục con trực tiếp của 1 thư mục.
    BrowseResult {
        folder_id: String,
        folder_name: String,
        entries: Vec<DriveEntry>,
    },
    BrowseError(String),

    /// Kết quả chọn thư mục lưu (từ hộp thoại chọn folder). `None` nếu người
    /// dùng bấm hủy hộp thoại.
    DestinationPicked(Option<PathBuf>),

    /// Đang quét đệ quy để đếm tổng số file trước khi tải (folder lớn có
    /// thể mất vài giây tới vài chục giây để quét xong).
    ScanProgress { found: usize },
    ScanError(String),

    JobStarted {
        total_files: usize,
        total_bytes: u64,
    },
    FileStarted {
        name: String,
    },
    /// Số byte MỚI tải thêm được kể từ lần báo trước (không phải tổng dồn
    /// của riêng 1 file) — thiết kế theo delta để cộng dồn an toàn ngay cả
    /// khi nhiều file đang tải song song cùng lúc.
    FileProgress {
        delta_bytes: u64,
    },
    FileDone {
        name: String,
    },
    /// 1 lần thử tải bị lỗi (kể cả lần cuối cùng trước khi bỏ cuộc) — dùng
    /// để hiện chi tiết lên nhật ký, giúp chẩn đoán các lỗi kiểu hạn mức.
    FileRetrying {
        name: String,
        attempt: u32,
        max_attempts: u32,
        error: String,
    },
    /// File đã có sẵn ở đích nên bỏ qua không tải lại (chính sách Skip).
    FileSkipped {
        name: String,
    },
    FileFailed {
        name: String,
        error: String,
    },
    JobFinished {
        succeeded: usize,
        failed: usize,
        cancelled: bool,
    },

    /// Kết quả tính dung lượng 1 thư mục (chỉ tính khi người dùng chủ động
    /// bấm xem, KHÔNG tự động tính khi duyệt, để không làm chậm việc duyệt
    /// file — 1 thư mục vài nghìn file có thể mất vài chục giây để quét).
    FolderSizeResult {
        folder_id: String,
        files: usize,
        bytes: u64,
    },
    FolderSizeError {
        folder_id: String,
    },

    // --- Đăng nhập Google (OAuth) + các thao tác ghi (đổi tên/xóa) ---
    /// Đăng nhập Google thành công.
    LoginSucceeded {
        tokens: crate::oauth::OAuthTokens,
        email: Option<String>,
    },
    LoginFailed(String),
    /// Access token vừa được tự động làm mới trong lúc thực hiện 1 thao
    /// tác ghi nào đó — cần lưu lại vào cấu hình để dùng cho lần sau, tránh
    /// phải đăng nhập lại chỉ vì access_token (sống ~1 giờ) đã hết hạn.
    TokensRefreshed(crate::oauth::OAuthTokens),

    FileRenamed {
        entry_id: String,
        new_name: String,
    },
    RenameFailed {
        entry_id: String,
        error: String,
    },

    /// Đã chuyển 1 file/thư mục vào Thùng rác trên Drive.
    FileTrashed {
        entry_id: String,
        name: String,
    },
    TrashFailed {
        entry_id: String,
        name: String,
        error: String,
    },

    BulkTrashProgress {
        done: usize,
        total: usize,
    },
    BulkTrashFinished {
        succeeded: usize,
        failed: usize,
    },

    // --- Kiểm tra chủ sở hữu trước khi xóa (Google chỉ cho CHỦ SỞ HỮU
    // chuyển thẳng vào Thùng rác — xem drive_api::WriteOperation::Trash) ---
    /// Đã biết được tài khoản đang đăng nhập có phải chủ sở hữu 1 file hay
    /// không (xóa đơn lẻ) — dùng để hiện đúng banner xác nhận.
    OwnershipChecked {
        entry: DriveEntry,
        info: crate::drive_api::OwnershipInfo,
    },
    OwnershipCheckFailed {
        entry: DriveEntry,
        error: String,
    },
    /// Đã kiểm tra xong chủ sở hữu cho CẢ danh sách xóa hàng loạt — hiện
    /// bảng tổng hợp (bao nhiêu file xóa thẳng được, bao nhiêu phải chuyển
    /// sang thư mục riêng) trước khi hỏi xác nhận cuối cùng.
    BulkOwnershipChecked(Vec<BulkDeletePlanItem>),
    BulkOwnershipCheckError(String),
    /// 1 file đã được CHUYỂN SANG thư mục riêng thay vì xóa thẳng (vì
    /// không phải chủ sở hữu) — phân biệt với `FileTrashed` để hiện đúng
    /// dòng nhật ký.
    FileMovedAside {
        entry_id: String,
        name: String,
    },
}

/// Kế hoạch xóa 1 file sau khi đã kiểm tra chủ sở hữu — nếu `owned_by_me`
/// là `false`, `start_bulk_delete` sẽ CHUYỂN file này sang thư mục riêng
/// thay vì gọi `trash_file` (sẽ chắc chắn bị Google từ chối).
#[derive(Debug, Clone)]
pub struct BulkDeletePlanItem {
    pub entry: DriveEntry,
    pub parent_id: Option<String>,
    pub owned_by_me: bool,
    pub owner_label: Option<String>,
}

pub struct ScannedItem {
    pub entry: DriveEntry,
    /// Đường dẫn tương đối (đã bao gồm tên file cuối cùng, kể cả đuôi file
    /// đã thêm cho Google Docs/Sheets/Slides) so với thư mục đích người dùng
    /// chọn, dùng để tái tạo lại cấu trúc thư mục trên Drive khi lưu xuống đĩa.
    pub relative_path: PathBuf,
}

/// Loại bỏ ký tự không hợp lệ làm tên file/thư mục trên Windows/macOS/Linux,
/// và bỏ khoảng trắng/dấu chấm ở cuối tên (Windows không cho phép).
pub fn sanitize_filename(name: &str) -> String {
    let mut s: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if (c as u32) < 0x20 => '_',
            c => c,
        })
        .collect();
    while s.ends_with('.') || s.ends_with(' ') {
        s.pop();
    }
    if s.is_empty() {
        s = "untitled".to_string();
    }
    s
}

/// Nếu `path` đã có trong `seen`, đổi thành "tên (2).ext", "tên (3).ext"...
/// cho tới khi tìm được đường dẫn chưa dùng. Cần thiết vì Google Drive
/// KHÔNG bắt tên file trong cùng 1 thư mục phải khác nhau, còn ổ đĩa thì có.
fn dedupe_path(path: PathBuf, seen: &mut HashSet<PathBuf>) -> PathBuf {
    if !seen.contains(&path) {
        seen.insert(path.clone());
        return path;
    }
    let parent = path.parent().map(|p| p.to_path_buf()).unwrap_or_default();
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let ext = path.extension().map(|s| s.to_string_lossy().to_string());
    let mut n = 2;
    loop {
        let candidate_name = match &ext {
            Some(e) => format!("{stem} ({n}).{e}"),
            None => format!("{stem} ({n})"),
        };
        let candidate = parent.join(candidate_name);
        if !seen.contains(&candidate) {
            seen.insert(candidate.clone());
            return candidate;
        }
        n += 1;
    }
}

/// Quét 1 danh sách entry đã biết (thường là danh sách đang hiển thị trên
/// GUI): file thì thêm thẳng vào kết quả, thư mục thì quét đệ quy tiếp.
///
/// Trả về dạng boxed-future vì đây là đệ quy tương hỗ với
/// `scan_folder_recursive` — Rust không cho phép 1 async fn gọi thẳng lại
/// chính nó (hoặc gọi vòng qua fn khác) mà không "boxed" ở đâu đó, do future
/// khi đó sẽ có kích thước vô hạn. Chỉ truyền tham chiếu DÙNG CHUNG (`&`,
/// không phải `&mut`) qua đệ quy để tránh mọi rắc rối borrow-checker khi
/// tái mượn (reborrow) qua nhiều lượt `.await` — việc khử trùng lặp tên file
/// được để dành làm ở bước riêng, sau khi đã quét xong toàn bộ cây (xem
/// `scan_all`), thay vì chia sẻ 1 `HashSet` có thể-ghi xuyên suốt đệ quy.
pub fn scan_mixed_entries<'a>(
    client: &'a DriveClient,
    entries: Vec<DriveEntry>,
    base_relative: &'a Path,
    found_counter: &'a AtomicUsize,
    progress_tx: &'a UnboundedSender<WorkerEvent>,
) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<ScannedItem>>> + Send + 'a>> {
    Box::pin(async move {
        let mut out = Vec::new();
        for entry in entries {
            if entry.is_folder {
                let sub_relative = base_relative.join(sanitize_filename(&entry.name));
                let mut sub_items =
                    scan_folder_recursive(client, &entry.id, &sub_relative, found_counter, progress_tx)
                        .await?;
                out.append(&mut sub_items);
            } else {
                let filename = sanitize_filename(&suggested_filename(&entry));
                out.push(ScannedItem {
                    relative_path: base_relative.join(filename),
                    entry,
                });
                let n = found_counter.fetch_add(1, Ordering::Relaxed) + 1;
                if n % 20 == 0 {
                    let _ = progress_tx.send(WorkerEvent::ScanProgress { found: n });
                }
            }
        }
        Ok(out)
    })
}

/// Lấy danh sách con trực tiếp của `folder_id` rồi quét tiếp (đệ quy).
pub fn scan_folder_recursive<'a>(
    client: &'a DriveClient,
    folder_id: &'a str,
    base_relative: &'a Path,
    found_counter: &'a AtomicUsize,
    progress_tx: &'a UnboundedSender<WorkerEvent>,
) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<ScannedItem>>> + Send + 'a>> {
    Box::pin(async move {
        let children = client.list_folder_children(folder_id).await?;
        scan_mixed_entries(client, children, base_relative, found_counter, progress_tx).await
    })
}

/// Quét toàn bộ cây thư mục bắt đầu từ 1 danh sách entry (file hoặc thư mục
/// trộn lẫn), gửi các mốc `ScanProgress` trong lúc quét, gửi 1 mốc cuối cùng
/// đảm bảo phản ánh đúng tổng số thật sự tìm được, rồi khử trùng lặp tên file
/// (Drive cho phép 2 file cùng tên trong 1 thư mục, ổ đĩa thì không).
pub async fn scan_all(
    client: &DriveClient,
    entries: Vec<DriveEntry>,
    progress_tx: &UnboundedSender<WorkerEvent>,
) -> anyhow::Result<Vec<ScannedItem>> {
    let found_counter = AtomicUsize::new(0);
    let mut result =
        scan_mixed_entries(client, entries, Path::new(""), &found_counter, progress_tx).await?;
    let _ = progress_tx.send(WorkerEvent::ScanProgress {
        found: found_counter.load(Ordering::Relaxed),
    });

    let mut seen = HashSet::new();
    for item in result.iter_mut() {
        item.relative_path = dedupe_path(item.relative_path.clone(), &mut seen);
    }
    Ok(result)
}

/// Tải nhiều file song song (giới hạn `concurrency` luồng cùng lúc), báo
/// tiến độ từng file + tổng thể qua `progress_tx`. Không panic khi 1 file lỗi
/// — ghi nhận là "failed" và tiếp tục các file còn lại.
pub async fn download_many(
    client: Arc<DriveClient>,
    items: Vec<ScannedItem>,
    dest_root: PathBuf,
    concurrency: usize,
    conflict_policy: ConflictPolicy,
    cancel: Arc<AtomicBool>,
    progress_tx: UnboundedSender<WorkerEvent>,
) {
    let total_files = items.len();
    let total_bytes: u64 = items.iter().filter_map(|i| i.entry.size).sum();
    let _ = progress_tx.send(WorkerEvent::JobStarted {
        total_files,
        total_bytes,
    });

    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let succeeded = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(items.len());
    for item in items {
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        let Ok(permit) = semaphore.clone().acquire_owned().await else {
            break;
        };
        let client = client.clone();
        let dest_root = dest_root.clone();
        let cancel = cancel.clone();
        let tx = progress_tx.clone();
        let succeeded = succeeded.clone();
        let failed = failed.clone();

        let handle = tokio::spawn(async move {
            let _permit = permit;
            let display_name = item.relative_path.to_string_lossy().to_string();
            let dest_path = dest_root.join(&item.relative_path);

            let _ = tx.send(WorkerEvent::FileStarted {
                name: display_name.clone(),
            });

            let tx_progress = tx.clone();
            let mut last_reported: u64 = 0;
            let tx_retry = tx.clone();
            let retry_name = display_name.clone();
            let result = client
                .download_file(
                    &item.entry,
                    &dest_path,
                    conflict_policy,
                    move |done, _total| {
                        let delta = done.saturating_sub(last_reported);
                        last_reported = done;
                        if delta > 0 {
                            let _ = tx_progress.send(WorkerEvent::FileProgress { delta_bytes: delta });
                        }
                    },
                    move |attempt, max_attempts, error| {
                        let _ = tx_retry.send(WorkerEvent::FileRetrying {
                            name: retry_name.clone(),
                            attempt,
                            max_attempts,
                            error: error.to_string(),
                        });
                    },
                    &cancel,
                )
                .await;

            match result {
                Ok(DownloadOutcome::Downloaded) => {
                    succeeded.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.send(WorkerEvent::FileDone { name: display_name });
                }
                Ok(DownloadOutcome::Skipped) => {
                    succeeded.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.send(WorkerEvent::FileSkipped { name: display_name });
                }
                Err(e) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.send(WorkerEvent::FileFailed {
                        name: display_name,
                        error: e.to_string(),
                    });
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    let _ = progress_tx.send(WorkerEvent::JobFinished {
        succeeded: succeeded.load(Ordering::Relaxed),
        failed: failed.load(Ordering::Relaxed),
        cancelled: cancel.load(Ordering::Relaxed),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_illegal_filesystem_characters() {
        assert_eq!(sanitize_filename("báo cáo: quý 3/2026"), "báo cáo_ quý 3_2026");
        assert_eq!(sanitize_filename("  ."), "untitled");
        assert_eq!(sanitize_filename("a<b>c"), "a_b_c");
    }

    #[test]
    fn dedupe_path_renames_on_collision() {
        let mut seen = HashSet::new();
        let p1 = dedupe_path(PathBuf::from("x/report.pdf"), &mut seen);
        let p2 = dedupe_path(PathBuf::from("x/report.pdf"), &mut seen);
        let p3 = dedupe_path(PathBuf::from("x/report.pdf"), &mut seen);
        assert_eq!(p1, PathBuf::from("x/report.pdf"));
        assert_eq!(p2, PathBuf::from("x/report (2).pdf"));
        assert_eq!(p3, PathBuf::from("x/report (3).pdf"));
    }
}
