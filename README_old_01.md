# GDrive Copier

Ứng dụng Desktop (Rust + egui) để **sao chép trực tiếp file/thư mục từ một
thư mục Google Drive đã chia sẻ công khai** (Public / "Anyone with the
link") ra ổ đĩa — không nén ZIP, không bắt người dùng đăng nhập hay OAuth.

Có thêm tính năng **đổi tên / chuyển vào Thùng rác** cho thư mục đã chia sẻ
quyền chỉnh sửa — phần này (và CHỈ phần này) cần đăng nhập Google 1 lần,
xem mục 2 bên dưới.

## Vì sao cần "API key" nếu đã nói là không cần đăng nhập?

Hai khái niệm này khác nhau:

- **OAuth / đăng nhập** = người dùng phải nhập tài khoản Google, cấp quyền
  truy cập Drive *của họ*. App này **không làm điều đó**.
- **API key** = một chuỗi tĩnh, miễn phí, do **bạn** (người build app) tự
  tạo **một lần** trên Google Cloud Console, dùng để app được phép gọi Google
  Drive API v3 ở chế độ chỉ-đọc dữ liệu **công khai**. Người dùng cuối
  (người dán link vào app để tải) không cần biết gì về API key, không cần
  đăng nhập bất cứ tài khoản nào.

Google API v3 có endpoint cũ dạng "duyệt web rồi bóc HTML" không cần key,
nhưng nó chỉ trả về tối đa ~50 mục/thư mục — không đủ cho thư mục cả nghìn
ảnh/video. Dùng API chính thức với key mới đọc được **toàn bộ** thư mục dù
lớn tới đâu (tự động phân trang).

## 1. Lấy Google API key (làm 1 lần, ~2 phút)

1. Vào [Google Cloud Console](https://console.cloud.google.com/).
2. Tạo project mới (hoặc chọn project có sẵn) ở góc trên bên trái.
3. Vào **APIs & Services → Library**, tìm **Google Drive API**, bấm **Enable**.
4. Vào **APIs & Services → Credentials → Create Credentials → API key**.
5. (Khuyến nghị) Bấm vào key vừa tạo → **Restrict key** → mục "API
   restrictions" chọn **Restrict key** → tick **Google Drive API** → Save.
   Việc này giới hạn key chỉ dùng được cho Drive API, an toàn hơn nếu lỡ lộ.
6. Copy chuỗi API key (dạng `AIzaSy...`).

Google Drive API có hạn mức miễn phí rất rộng rãi cho việc đọc dữ liệu
public (đủ dùng thoải mái cho cá nhân/nhóm nhỏ); nếu dùng ở quy mô lớn hơn,
xem thêm hạn mức tại trang **APIs & Services → Quotas** trong Console.

## 2. (Tùy chọn) Lấy OAuth Client ID — nếu muốn đổi tên/xóa file

Chỉ cần bước này nếu muốn dùng tính năng **đổi tên** hoặc **chuyển vào
Thùng rác**. Nếu chỉ cần tải file, bỏ qua mục này.

1. Trong **cùng project** đã tạo ở Bước 1, vào **APIs & Services →
   OAuth consent screen**:
   - User Type: chọn **External** (trừ khi bạn dùng Google Workspace).
   - Điền tên app, email liên hệ bất kỳ → Save.
   - Mục **Test users**: bấm **Add users**, thêm đúng địa chỉ Gmail bạn sẽ
     dùng để đăng nhập trong app. **Bắt buộc** — thiếu bước này Google sẽ
     từ chối đăng nhập.
2. Vào **APIs & Services → Credentials → Create Credentials → OAuth
   client ID**.
   - Application type: **Desktop app**.
   - Đặt tên tùy ý → Create.
3. Copy **Client ID** (dạng `...apps.googleusercontent.com`) và **Client
   Secret** hiện ra.
4. Mở app → mục Cài đặt → dán cả 2 giá trị vào ô **OAuth Client ID** /
   **OAuth Client Secret** → bấm **Đăng nhập Google**. Trình duyệt sẽ mở ra;
   vì app chưa qua kiểm duyệt của Google (chỉ dùng cá nhân) nên sẽ thấy cảnh
   báo "App chưa được xác minh" — bấm **Advanced/Nâng cao → Đi tới [tên
   app] (không an toàn)** để tiếp tục. Đây là cảnh báo bình thường với app
   tự build cho mục đích cá nhân, không phải lỗi.

**Lưu ý quan trọng:** vì project ở trạng thái "Testing" (chưa gửi Google
duyệt) và quyền xin là quyền rộng (toàn bộ Drive), Google chỉ cấp phiên đăng
nhập sống **7 ngày** — sau đó phải bấm **Đăng nhập Google** lại (không mất
gì, chỉ là thao tác lại vài giây). Đây là giới hạn từ phía Google, không
phải lỗi của app.

## 3. Cài Rust (nếu máy chưa có)

Cài qua [rustup.rs](https://rustup.rs) (Windows/macOS/Linux đều hỗ trợ).
Sau khi cài xong, kiểm tra:

```bash
cargo --version
```

## 4. Build & chạy

```bash
cd gdrive-copier
cargo build --release
./target/release/gdrive-copier        # Linux/macOS
# hoặc: .\target\release\gdrive-copier.exe   (Windows)
```

Lần chạy đầu tiên, app sẽ hiện màn hình yêu cầu dán API key (đã lấy ở Bước
1) → bấm **Lưu**. Từ lần sau app tự nhớ, không cần nhập lại (lưu ở thư mục
config chuẩn của hệ điều hành, xem `src/config.rs`).

## 5. Cách dùng — tải file

1. Dán link thư mục Drive (dạng `https://drive.google.com/drive/folders/...`,
   phải là thư mục đã chia sẻ **"Anyone with the link"**) → bấm **Mở**.
2. Nhấp đúp vào tên 1 thư mục để đi vào bên trong; dùng breadcrumb ở trên để
   quay lại thư mục cha.
3. Chọn file/thư mục muốn tải bằng ô tick ở đầu mỗi dòng:
   - Giữ **Shift** khi tick để chọn nhanh cả một khoảng (giống Explorer).
   - Ô **Chọn tất cả** ở đầu danh sách để chọn/bỏ chọn toàn bộ.
   - Nút **Tải N mục đã chọn (~X GB)** hiện ra khi có mục được chọn, cho biết
     tổng dung lượng ước tính (thư mục nào chưa bấm "xem dung lượng" thì sẽ
     được ghi chú riêng, không tính nhầm là 0).
   - Vẫn có thể bấm nút **Tải** ngay trên từng dòng để tải riêng lẻ, không
     cần tick chọn trước.
4. Với thư mục con, có thể bấm liên kết **"xem dung lượng"** để biết trước số
   file + tổng dung lượng của cả thư mục đó (chỉ tính khi bấm, không tự động,
   để không làm chậm việc duyệt các thư mục lớn).
5. Lần đầu bấm **Tải**, app sẽ tự mở hộp thoại chọn thư mục lưu (chưa cần
   chọn trước) — chọn xong sẽ tự động tải luôn, không cần bấm Tải lại lần 2.
   Các lần sau tái sử dụng đúng thư mục đó; có thể đổi qua nút **Chọn thư
   mục...** bất kỳ lúc nào.
6. Mục **"Nếu trùng tên file"** cho chọn cách xử lý khi file đích đã có sẵn
   trên ổ đĩa: **Bỏ qua** (mặc định, giữ file cũ — phù hợp khi chạy lại 1 job
   bị dừng giữa chừng), **Ghi đè**, hoặc **Đổi tên file mới** (`_v1`, `_v2`...
   giữ cả 2 bản).
7. Trong lúc tải: thanh tiến độ hiện số file đã xong, tổng dung lượng, **tốc
   độ tải hiện tại**, và **thời gian còn lại ước tính**. Có thể bấm **Hủy**
   giữa chừng; job có thể chạy lại sau đó mà không tải lại các file đã xong
   (tùy theo lựa chọn ở mục 6).

## 6. Cách dùng — đổi tên / xóa file (cần đăng nhập, xem Mục 2)

Chỉ hiện các nút này SAU KHI đã đăng nhập Google thành công.

- **Đổi tên**: bấm ✎ trên dòng file/thư mục muốn đổi → gõ tên mới → Enter
  hoặc bấm ✓ để xác nhận, ✕ để hủy.
- **Xóa 1 file**: bấm 🗑 trên dòng đó → xác nhận trong banner hiện ra ("Chuyển
  vào Thùng rác?"). App **không xóa vĩnh viễn** — chỉ chuyển vào Thùng rác
  của Drive, có thể khôi phục lại trong 30 ngày nếu lỡ tay.
- **Xóa hàng loạt theo danh sách tên**: mở mục "Xóa hàng loạt theo danh sách
  tên" bên dưới danh sách file → dán/nhập tên các file cần xóa, hoặc **kéo
  thả 1 file `.txt`** chứa danh sách vào cửa sổ app (tự đọc và nối vào ô
  nhập, không xóa nội dung đã gõ sẵn) → bấm **Tìm & xem trước** → kiểm tra
  danh sách khớp được liệt kê ra → bấm **Chuyển N file vào Thùng rác** để
  xác nhận xóa thật.
  - Định dạng danh sách linh hoạt: mỗi tên 1 dòng, hoặc cách nhau bằng dấu
    phẩy (`,`), chấm phẩy (`;`), tab, hoặc khoảng trắng — trộn lẫn nhiều
    kiểu trong cùng danh sách cũng được (ví dụ `a.jpg, b.mp4\nc.png; d.pdf`).
    Tên file CHỨA khoảng trắng (ví dụ `ảnh cưới.jpg`) vẫn được nhận đúng
    miễn là nó không bị cách bằng khoảng trắng với 1 tên khác trên cùng
    dòng — an toàn nhất là mỗi tên như vậy tự xuống dòng riêng.
  - Phải khớp CHÍNH XÁC tên đang hiển thị trong thư mục đang xem (đã tự
    bỏ khoảng trắng dư ở đầu/cuối mỗi tên).
- Đổi tên/xóa chỉ áp dụng cho các mục trong **thư mục đang mở**, không đệ quy
  vào thư mục con.

## Giới hạn cần biết

- Chỉ đọc được nội dung đã chia sẻ **công khai**. Thư mục riêng tư sẽ báo lỗi
  quyền truy cập (403) — đây là hành vi đúng, không phải bug.
- File Google Docs/Sheets/Slides không có "byte gốc" để tải thẳng, app tự
  xuất (export) sang `.docx` / `.xlsx` / `.pptx` (hoặc `.pdf` cho các loại
  Google-app khác như Forms, Apps Script...).
- Google có thể thay đổi chính sách/hạn mức API theo thời gian; nếu gặp lỗi
  "quota" thường xuyên, kiểm tra mục Quotas trong Cloud Console.
- **Tải file lớn (video vài trăm MB — vài GB) đôi khi bị ngắt giữa chừng**
  (lỗi kiểu "mất kết nối khi đang tải" hoặc HTTP 403) — đây là giới hạn băng
  thông/egress phía Google khi tải nhiều dữ liệu lớn liên tục qua API, không
  phải lỗi của app. App tự động thử lại tối đa 6 lần, mỗi lần **tải tiếp từ
  đúng chỗ đã dừng** (không tải lại từ đầu) qua HTTP Range — kể cả khi bạn
  tắt app và mở lại sau đó. Nếu vẫn thất bại sau 6 lần (ví dụ Google giới hạn
  theo giờ), cứ đợi một lúc rồi bấm **Tải** lại — các file đã tải xong sẽ tự
  động được bỏ qua (theo chính sách "Bỏ qua" ở mục 6), chỉ những file chưa
  xong mới được tải tiếp.
- **Đổi tên/xóa cần quyền Editor** trở lên trên file/thư mục đó (chủ sở hữu
  chia sẻ ở chế độ "Người chỉnh sửa", không phải "Người xem"). Nếu thiếu
  quyền, Google trả lỗi 403 — không phải lỗi của app.
- Phiên đăng nhập Google chỉ sống **7 ngày** (giới hạn của Google cho app ở
  trạng thái "Testing", xem Mục 2) — hết hạn thì đăng nhập lại là dùng được
  ngay, không mất cấu hình gì khác.

## Cấu trúc mã nguồn

```
src/
  main.rs       Điểm khởi động, cấu hình cửa sổ eframe
  app.rs        Toàn bộ giao diện egui + điều phối trạng thái
  drive_api.rs  Client Drive API v3: đọc bằng API key (list/get/download/
                export) + ghi bằng OAuth (đổi tên/chuyển vào Thùng rác)
  downloader.rs Quét đệ quy thư mục + tải song song có giới hạn luồng
  config.rs     Lưu/đọc API key, token OAuth, cấu hình vào thư mục config HĐH
  oauth.rs      Đăng nhập Google (OAuth 2.0 + PKCE, RFC 8252) cho Desktop app
assets/
  NotoSans-Regular.ttf   Font nhúng sẵn để hiển thị đúng dấu tiếng Việt
                         (egui không có sẵn font hỗ trợ tiếng Việt).
  NotoSans-LICENSE.txt   Giấy phép SIL OFL 1.1 của font trên.
```
