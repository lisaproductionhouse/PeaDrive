//! Lưu/đọc cấu hình app (API key, thư mục lưu gần nhất, số luồng tải song
//! song) vào 1 file JSON trong thư mục config chuẩn của hệ điều hành:
//! - Windows: `%APPDATA%\gdrive-copier\config\config.json`
//! - macOS:   `~/Library/Application Support/com.gdrive-copier.GDriveCopier/config.json`
//! - Linux:   `~/.config/gdrive-copier/config.json`
//!
//! Đây KHÔNG phải là đăng nhập/OAuth của người dùng: API key là một chuỗi
//! tĩnh do người triển khai app tự tạo trên Google Cloud Console (xem
//! README.md), dùng chung cho toàn bộ app, không gắn với tài khoản Google
//! của người dùng cuối.

use crate::oauth::OAuthTokens;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Cách xử lý khi file đích đã tồn tại sẵn trên ổ đĩa (từ lần tải trước,
/// hoặc người dùng đã có sẵn file cùng tên).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictPolicy {
    /// Giữ nguyên file đã có, không tải đè lên (mặc định — phù hợp khi
    /// chạy lại 1 job bị dừng giữa chừng, không tải lại từ đầu).
    Skip,
    /// Luôn tải về và ghi đè lên file cũ.
    Overwrite,
    /// Giữ file cũ, lưu bản tải mới với tên khác: `ten_v1.ext`, nếu vẫn
    /// trùng thì `ten_v2.ext`, v.v.
    Rename,
}

impl Default for ConflictPolicy {
    fn default() -> Self {
        Self::Skip
    }
}

impl ConflictPolicy {
    pub fn label(self) -> &'static str {
        match self {
            Self::Skip => "Bỏ qua (giữ file cũ)",
            Self::Overwrite => "Ghi đè",
            Self::Rename => "Đổi tên file mới (_v1, _v2...)",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub api_key: Option<String>,
    pub last_destination: Option<PathBuf>,
    #[serde(default = "default_concurrency")]
    pub max_concurrent_downloads: usize,
    #[serde(default)]
    pub conflict_policy: ConflictPolicy,

    // --- Đăng nhập Google (OAuth) — CHỈ cần nếu dùng tính năng đổi tên/xóa
    // file. Không liên quan gì tới api_key ở trên (vốn dùng để tải file,
    // không cần đăng nhập). Xem README.md mục "Đổi tên / xóa file".
    /// OAuth Client ID (dạng "...apps.googleusercontent.com"), tự tạo trên
    /// Google Cloud Console, loại "Desktop app".
    pub oauth_client_id: Option<String>,
    pub oauth_client_secret: Option<String>,
    /// Token sau khi đăng nhập thành công; `None` nghĩa là chưa đăng nhập.
    pub oauth_tokens: Option<OAuthTokens>,
    /// Email tài khoản đang đăng nhập, chỉ để hiển thị lên giao diện.
    pub oauth_email: Option<String>,
}

impl Default for AppConfig {
    // Viết tay thay vì #[derive(Default)]: nếu derive, "max_concurrent_downloads"
    // sẽ mặc định về 0 (Default của usize) thay vì 4 — vẫn không panic gì
    // (clamp(1, 16) sau đó sẽ kéo về 1) nhưng vô tình khiến lần chạy đầu tiên
    // (chưa có file config) tải tuần tự thay vì song song 4 luồng như ý định.
    fn default() -> Self {
        Self {
            api_key: None,
            last_destination: None,
            max_concurrent_downloads: default_concurrency(),
            conflict_policy: ConflictPolicy::default(),
            oauth_client_id: None,
            oauth_client_secret: None,
            oauth_tokens: None,
            oauth_email: None,
        }
    }
}

fn default_concurrency() -> usize {
    4
}

fn config_file_path() -> anyhow::Result<PathBuf> {
    let proj_dirs = directories::ProjectDirs::from("com", "gdrive-copier", "GDriveCopier")
        .ok_or_else(|| anyhow::anyhow!("Không xác định được thư mục cấu hình của hệ điều hành"))?;
    let dir = proj_dirs.config_dir().to_path_buf();
    std::fs::create_dir_all(&dir)?;
    Ok(dir.join("config.json"))
}

impl AppConfig {
    /// Đọc config đã lưu; nếu chưa từng lưu hoặc file lỗi, trả về config rỗng
    /// (không panic, không làm app không mở lên được).
    pub fn load() -> Self {
        Self::try_load().unwrap_or_default()
    }

    fn try_load() -> anyhow::Result<Self> {
        let path = config_file_path()?;
        let text = std::fs::read_to_string(path)?;
        let cfg: AppConfig = serde_json::from_str(&text)?;
        Ok(cfg)
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let path = config_file_path()?;
        let text = serde_json::to_string_pretty(self)?;
        std::fs::write(path, text)?;
        Ok(())
    }
}
