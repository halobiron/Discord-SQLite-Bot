# 📡 CORS Alarm System - Hệ thống giám sát trạm CORS

Hệ thống giám sát và cảnh báo tự động cho các trạm CORS với Discord bot và báo cáo thống kê.

## ✨ Tính năng chính

### 🤖 Discord Bot Commands
- **`/rp [tỉnh]`** - Báo cáo tình trạng trạm theo tỉnh hoặc toàn bộ
- **`/fr [tỉnh/trạm]`** - Báo cáo tỷ lệ Fixed Rate theo tỉnh, trạm hoặc tổng thể
- **`/bccl`** - Tạo báo cáo chất lượng hàng giờ
- **`/addwhitelist <trạm1,trạm2>`** - Thêm trạm vào danh sách trắng
- **`/cleanup`** - Dọn dẹp database (xóa dữ liệu cũ hơn 6 tháng)
- **`/ping`** - Kiểm tra tình trạng bot

### 📊 Giám sát tự động
- **Báo cáo trạng thái trạm** - Kiểm tra trạm online/offline mỗi 15 phút
- **Báo cáo Fixed Rate** - Thống kê tỷ lệ cố định mỗi 5 phút và 15 phút
- **Báo cáo hàng giờ** - Tổng hợp chất lượng hệ thống
- **Cảnh báo Discord** - Gửi thông báo tự động khi có vấn đề

### 💾 Quản lý dữ liệu
- **SQLite Database** - Lưu trữ dữ liệu local, tránh phụ thuộc mạng
- **API Integration** - Kết nối với RTK API để lấy dữ liệu thời gian thực
- **Data Cleanup** - Tự động xóa dữ liệu cũ để tiết kiệm dung lượng

## 🚀 Cài đặt và Thiết lập

### 1. Cài đặt Dependencies
```bash
pip install -r requirements.txt
```

### 2. Thiết lập Environment Variables

**Tạo file `.env` từ template:**
```bash
copy .env.example .env
```

**Chỉnh sửa file `.env` với thông tin thực tế:**
```env
# Discord Configuration
DISCORD_BOT_TOKEN=your_discord_bot_token_here
DISCORD_WEBHOOK_URL=your_discord_webhook_url_here

# API Configuration  
API_ACCESS_KEY=your_api_access_key_here
API_SECRET_KEY=your_api_secret_key_here
API_SIGN_METHOD=HmacSHA256
API_BASE_URL=http://rtk.taikhoandodac.vn:8090

# Google Service Account
GOOGLE_SERVICE_ACCOUNT_FILE=your_service_account_file.json
```

### 3. Chạy hệ thống
```bash
python discord_bot.py
```

## 📖 Hướng dẫn sử dụng

### Discord Commands

#### 📊 Báo cáo trạng thái trạm
```
/rp                    # Báo cáo tất cả trạm
/rp HN                 # Báo cáo trạm tỉnh Hà Nội (bắt đầu HN)
/rp HCM                # Báo cáo trạm TP.HCM (bắt đầu HCM)
```

#### 📈 Báo cáo Fixed Rate
```
/fr                    # Báo cáo tổng thể Fixed Rate
/fr HN                 # Báo cáo Fixed Rate tỉnh Hà Nội
/fr HNI1               # Báo cáo Fixed Rate trạm HNI1
```

#### ⏰ Báo cáo định kỳ
```
/bccl                  # Tạo báo cáo chất lượng hàng giờ ngay lập tức
```

#### ⚙️ Quản lý hệ thống
```
/addwhitelist HNI1,HNI2,PYN1    # Thêm trạm vào whitelist
/cleanup                         # Dọn dẹp database
/ping                           # Kiểm tra bot
```

### Giám sát tự động

Hệ thống sẽ tự động:
- **Mỗi 5 phút**: Thu thập dữ liệu Fixed Rate
- **Mỗi 15 phút**: Kiểm tra trạng thái trạm và báo cáo nếu có thay đổi
- **Mỗi giờ**: Gửi báo cáo chất lượng tổng thể
- **Hàng ngày**: Dọn dẹp dữ liệu cũ

### Thông tin báo cáo

#### Báo cáo trạng thái trạm
- **🟢 Online**: Trạm hoạt động bình thường
- **🔴 Offline**: Trạm mất kết nối
- **⚪ Unknown**: Không có thông tin

#### Báo cáo Fixed Rate
- **📊 Tỷ lệ Fixed**: Phần trăm người dùng có tín hiệu ổn định
- **👥 Users**: Số người dùng trung bình
- **✅ Fixed Users**: Số người dùng có tín hiệu Fixed
- **📡 Stations**: Số trạm có người dùng

## 🔧 Cấu trúc File

```
CORS_Alarm/
├── discord_bot.py          # Bot Discord chính
├── monitor_sqlite.py       # Module giám sát SQLite
├── database.py            # Quản lý database
├── requirements.txt       # Dependencies
├── .env                   # Biến môi trường (không commit)
├── .env.example          # Template environment
├── .gitignore            # File ignore Git
├── monitoring.db         # Database SQLite
└── *-alert-*.json   # Google Service Account
```

## 🔒 Bảo mật

- ✅ Tất cả thông tin nhạy cảm được lưu trong file `.env`
- ✅ File `.env` và credentials đã được thêm vào `.gitignore`
- ✅ Không hardcode token/key trong source code
- ✅ Validation biến môi trường khi khởi động

## 📝 Log và Debug

- **bot.log**: Log hoạt động Discord bot
- **Database stats**: Sử dụng `/cleanup` để xem thống kê database
- **Error handling**: Tất cả lỗi được log và báo cáo qua Discord

## 🆘 Troubleshooting

### Bot không phản hồi
1. Kiểm tra `DISCORD_BOT_TOKEN` trong file `.env`
2. Đảm bảo bot đã được invite vào server với quyền Slash Commands
3. Kiểm tra log file `bot.log`

### Không nhận được báo cáo tự động
1. Kiểm tra `DISCORD_WEBHOOK_URL` trong file `.env`
2. Đảm bảo API credentials (`API_ACCESS_KEY`, `API_SECRET_KEY`) đúng
3. Kiểm tra kết nối mạng tới API server

### Database lỗi
1. Xóa file `monitoring.db` để tạo database mới
2. Chạy lại `python discord_bot.py`
3. Sử dụng `/cleanup` để dọn dẹp dữ liệu cũ

---

