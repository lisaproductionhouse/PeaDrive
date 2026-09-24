//! Đăng nhập Google bằng OAuth 2.0 dành cho "Desktop app" (RFC 8252), CHỈ
//! dùng cho các tính năng CHỈNH SỬA (đổi tên, xóa) — phần tải file ở
//! `drive_api.rs` không đụng tới module này, vẫn hoạt động bằng API key như
//! cũ, không cần đăng nhập.
//!
//! Luồng chuẩn cho ứng dụng cài trên máy (không phải web server):
//! 1. Sinh cặp PKCE (code_verifier + code_challenge) và 1 chuỗi `state`
//!    ngẫu nhiên để chống CSRF.
//! 2. Mở trình duyệt mặc định tới trang đăng nhập/đồng ý của Google.
//! 3. Google chuyển hướng trình duyệt về `http://127.0.0.1:{cổng}/...` kèm
//!    `code` — app có 1 server HTTP tạm trên máy đang chờ sẵn để bắt lại.
//! 4. Đổi `code` (+ `code_verifier`) lấy `access_token` + `refresh_token`.
//! 5. Lưu `refresh_token` để những lần sau không phải đăng nhập lại;
//!    `access_token` hết hạn sau khoảng 1 giờ thì tự làm mới bằng bước 4
//!    (dùng `refresh_token`, không cần mở trình duyệt lại).
//!
//! Tham khảo: https://developers.google.com/identity/protocols/oauth2/native-app

use base64::Engine;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const AUTH_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";
/// Toàn quyền đọc/ghi Drive — bắt buộc phải rộng cỡ này vì tính năng đổi
/// tên/xóa cần thao tác trên file người khác chia sẻ (không phải file do
/// chính app tạo ra), nên scope hẹp như `drive.file` không dùng được.
pub const DRIVE_SCOPE: &str = "https://www.googleapis.com/auth/drive";
/// Chỉ để hiển thị email tài khoản đang đăng nhập lên giao diện (an toàn
/// hơn khi sắp thực hiện thao tác xóa) — không ảnh hưởng gì tới quyền Drive.
const EMAIL_SCOPE: &str = "https://www.googleapis.com/auth/userinfo.email";
const USERINFO_ENDPOINT: &str = "https://www.googleapis.com/oauth2/v3/userinfo";

const PKCE_CHARSET: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
const STATE_CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

fn random_string(charset: &[u8], len: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..len)
        .map(|_| charset[rng.gen_range(0..charset.len())] as char)
        .collect()
}

struct Pkce {
    verifier: String,
    challenge: String,
}

fn generate_pkce() -> Pkce {
    let verifier = random_string(PKCE_CHARSET, 64);
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize());
    Pkce { verifier, challenge }
}

/// Token đã lưu, đủ để dùng lại giữa các lần mở app mà không cần đăng nhập
/// lại (chỉ cần đăng nhập lại nếu người dùng chủ động "Đăng xuất", hoặc
/// refresh_token bị Google thu hồi).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access_token: String,
    pub refresh_token: String,
    /// Mốc thời gian Unix (giây) mà access_token hết hạn.
    pub expires_at: u64,
}

impl OAuthTokens {
    /// Coi như hết hạn sớm hơn 60 giây so với mốc thật, để chừa thời gian
    /// gọi API xong trước khi access_token thật sự bị Google từ chối.
    pub fn is_expired(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        now + 60 >= self.expires_at
    }
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
    /// Chỉ có ở lần trao đổi code ĐẦU TIÊN; khi refresh thường sẽ không có
    /// trường này trong phản hồi (Google giữ nguyên refresh_token cũ).
    refresh_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenErrorResponse {
    error: String,
    #[serde(default)]
    error_description: String,
}

fn tokens_from_response(resp: TokenResponse, fallback_refresh_token: Option<&str>) -> anyhow::Result<OAuthTokens> {
    let refresh_token = resp
        .refresh_token
        .or_else(|| fallback_refresh_token.map(|s| s.to_string()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Google không trả về refresh_token. Hãy đăng nhập lại từ đầu (nút Đăng xuất rồi Đăng nhập lại)."
            )
        })?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Ok(OAuthTokens {
        access_token: resp.access_token,
        refresh_token,
        expires_at: now + resp.expires_in,
    })
}

async fn post_token_request(
    http: &reqwest::Client,
    params: &[(&str, &str)],
) -> anyhow::Result<TokenResponse> {
    let resp = http
        .post(TOKEN_ENDPOINT)
        .form(params)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Lỗi kết nối tới Google: {e}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let message = serde_json::from_str::<TokenErrorResponse>(&text)
            .map(|e| {
                if e.error_description.is_empty() {
                    e.error
                } else {
                    format!("{}: {}", e.error, e.error_description)
                }
            })
            .unwrap_or_else(|_| format!("HTTP {}", status.as_u16()));
        anyhow::bail!("Google từ chối yêu cầu đăng nhập: {message}");
    }
    serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("Không đọc được phản hồi đăng nhập từ Google: {e}"))
}

/// Đổi authorization code lấy access/refresh token (bước cuối của lần đăng
/// nhập ĐẦU TIÊN).
async fn exchange_code(
    http: &reqwest::Client,
    client_id: &str,
    client_secret: &str,
    code: &str,
    code_verifier: &str,
    redirect_uri: &str,
) -> anyhow::Result<OAuthTokens> {
    let params = [
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("code", code),
        ("code_verifier", code_verifier),
        ("grant_type", "authorization_code"),
        ("redirect_uri", redirect_uri),
    ];
    let resp = post_token_request(http, &params).await?;
    tokens_from_response(resp, None)
}

/// Lấy access_token mới bằng refresh_token đã lưu (không cần mở trình
/// duyệt).
pub async fn refresh_access_token(
    http: &reqwest::Client,
    client_id: &str,
    client_secret: &str,
    old_tokens: &OAuthTokens,
) -> anyhow::Result<OAuthTokens> {
    let params = [
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("refresh_token", old_tokens.refresh_token.as_str()),
        ("grant_type", "refresh_token"),
    ];
    let resp = post_token_request(http, &params).await?;
    tokens_from_response(resp, Some(&old_tokens.refresh_token))
}

/// Nếu token đã hết hạn (hoặc sắp hết), tự làm mới; nếu chưa thì trả về
/// nguyên trạng. Trả về token (đã cập nhật hoặc không) để bên gọi tự lưu
/// lại vào cấu hình nếu có thay đổi.
pub async fn ensure_fresh(
    http: &reqwest::Client,
    client_id: &str,
    client_secret: &str,
    tokens: OAuthTokens,
) -> anyhow::Result<OAuthTokens> {
    if tokens.is_expired() {
        refresh_access_token(http, client_id, client_secret, &tokens).await
    } else {
        Ok(tokens)
    }
}

fn parse_query(query: &str) -> std::collections::HashMap<String, String> {
    query
        .split('&')
        .filter_map(|pair| {
            let mut it = pair.splitn(2, '=');
            let k = it.next()?;
            let v = it.next().unwrap_or("");
            let decode = |s: &str| -> String {
                // Giải mã percent-encoding tối thiểu (đủ cho code/state/error
                // của Google, vốn chỉ gồm ký tự an toàn hoặc dùng '%XX').
                let mut out = String::with_capacity(s.len());
                let bytes = s.as_bytes();
                let mut i = 0;
                while i < bytes.len() {
                    if bytes[i] == b'%' && i + 2 < bytes.len() {
                        if let Ok(byte) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                            out.push(byte as char);
                            i += 3;
                            continue;
                        }
                    }
                    out.push(bytes[i] as char);
                    i += 1;
                }
                out
            };
            Some((decode(k), decode(v)))
        })
        .collect()
}

fn extract_query_from_request_line(line: &str) -> Option<String> {
    let path_and_query = line.split_whitespace().nth(1)?;
    let (_, query) = path_and_query.split_once('?')?;
    Some(query.to_string())
}

const SUCCESS_HTML: &str =
    "<html><body style=\"font-family: sans-serif; padding: 40px;\"><h2>Đã đăng nhập xong ✔</h2><p>Bạn có thể đóng tab này và quay lại ứng dụng.</p></body></html>";
const DENIED_HTML: &str =
    "<html><body style=\"font-family: sans-serif; padding: 40px;\"><h2>Đã hủy đăng nhập</h2><p>Bạn có thể đóng tab này.</p></body></html>";

async fn wait_for_redirect_once(
    listener: &TcpListener,
    expected_state: &str,
) -> anyhow::Result<String> {
    let (mut stream, _) = listener.accept().await?;
    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await?;
    let request = String::from_utf8_lossy(&buf[..n]).to_string();
    let first_line = request.lines().next().unwrap_or("");
    let query = extract_query_from_request_line(first_line)
        .ok_or_else(|| anyhow::anyhow!("Không đọc được phản hồi từ Google"))?;
    let params = parse_query(&query);

    let is_denied = params.contains_key("error");
    let html = if is_denied { DENIED_HTML } else { SUCCESS_HTML };
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        html.len(),
        html
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;

    if let Some(err) = params.get("error") {
        anyhow::bail!("Bạn đã từ chối cấp quyền ({err})");
    }
    let code = params
        .get("code")
        .ok_or_else(|| anyhow::anyhow!("Phản hồi từ Google thiếu mã xác thực"))?;
    let state = params
        .get("state")
        .ok_or_else(|| anyhow::anyhow!("Phản hồi từ Google thiếu tham số state"))?;
    if state != expected_state {
        anyhow::bail!("Tham số state không khớp — có thể bị tấn công, đã hủy đăng nhập");
    }
    Ok(code.clone())
}

/// Chạy toàn bộ luồng đăng nhập: mở trình duyệt, chờ redirect, đổi code lấy
/// token. Hàm này CHẶN (await) tới khi người dùng hoàn tất (hoặc hủy) trên
/// trình duyệt — bên gọi (`app.rs`) chạy nó trong 1 tác vụ nền riêng, không
/// chặn giao diện.
pub async fn login(
    http: &reqwest::Client,
    client_id: String,
    client_secret: String,
) -> anyhow::Result<OAuthTokens> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| anyhow::anyhow!("Không mở được cổng cục bộ để nhận đăng nhập: {e}"))?;
    let port = listener.local_addr()?.port();
    let redirect_uri = format!("http://127.0.0.1:{port}");

    let pkce = generate_pkce();
    let state = random_string(STATE_CHARSET, 32);

    let mut auth_url = url::Url::parse(AUTH_ENDPOINT)?;
    auth_url
        .query_pairs_mut()
        .append_pair("client_id", &client_id)
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("response_type", "code")
        .append_pair("scope", &format!("{DRIVE_SCOPE} {EMAIL_SCOPE}"))
        .append_pair("code_challenge", &pkce.challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", &state)
        .append_pair("access_type", "offline")
        .append_pair("prompt", "consent");

    open::that(auth_url.as_str())
        .map_err(|e| anyhow::anyhow!("Không mở được trình duyệt: {e}"))?;

    let code = wait_for_redirect_once(&listener, &state).await?;

    exchange_code(http, &client_id, &client_secret, &code, &pkce.verifier, &redirect_uri).await
}

#[derive(Debug, Deserialize)]
struct UserInfo {
    email: Option<String>,
}

/// Lấy email tài khoản đang đăng nhập, CHỈ để hiển thị lên giao diện cho
/// người dùng biết chắc mình đang thao tác bằng tài khoản nào trước khi xóa
/// file — không quan trọng tới mức phải làm cả app fail nếu gọi lỗi, nên
/// trả `None` thay vì lỗi nếu có trục trặc gì đó (mạng chập chờn, quyền
/// thiếu scope email...).
pub async fn fetch_user_email(http: &reqwest::Client, access_token: &str) -> Option<String> {
    let resp = http
        .get(USERINFO_ENDPOINT)
        .bearer_auth(access_token)
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let info: UserInfo = resp.json().await.ok()?;
    info.email
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_matches_rfc7636_example() {
        // Cùng vector chuẩn trong RFC 7636 Appendix B.
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let mut hasher = Sha256::new();
        hasher.update(verifier.as_bytes());
        let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize());
        assert_eq!(challenge, "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
    }

    #[test]
    fn is_expired_respects_60s_safety_margin() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let almost_expired = OAuthTokens {
            access_token: "x".into(),
            refresh_token: "y".into(),
            expires_at: now + 30, // còn 30s -> trong vùng đệm 60s -> coi như hết hạn
        };
        assert!(almost_expired.is_expired());

        let fresh = OAuthTokens {
            access_token: "x".into(),
            refresh_token: "y".into(),
            expires_at: now + 3600,
        };
        assert!(!fresh.is_expired());
    }

    #[test]
    fn tokens_from_response_falls_back_to_old_refresh_token_when_missing() {
        let resp = TokenResponse {
            access_token: "new-access".into(),
            expires_in: 3600,
            refresh_token: None,
        };
        let tokens = tokens_from_response(resp, Some("old-refresh")).unwrap();
        assert_eq!(tokens.refresh_token, "old-refresh");
        assert_eq!(tokens.access_token, "new-access");
    }

    #[test]
    fn tokens_from_response_errors_when_no_refresh_token_available_at_all() {
        let resp = TokenResponse {
            access_token: "new-access".into(),
            expires_in: 3600,
            refresh_token: None,
        };
        assert!(tokens_from_response(resp, None).is_err());
    }
}
