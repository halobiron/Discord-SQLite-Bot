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

### 2. Chỉnh sửa file `.env` với thông tin thực tế

### 3. Chạy hệ thống
```bash
python discord_bot.py
```

### 4. Chạy liên tục trên server (khuyến nghị dùng Systemd)
Để bot chạy ổn định, tự khởi động lại khi lỗi hoặc khi reboot server, bạn nên dùng `systemd`.

#### Bước 1: Tạo file cấu hình service
Chạy lệnh sau để tạo file:
```bash
sudo nano /etc/systemd/system/discord-bot.service
```

#### Bước 2: Dán nội dung sau vào (Sửa đường dẫn phù hợp)
```ini
[Unit]
Description=Discord SQLite Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/CORS_Alarm
ExecStart=/root/CORS_Alarm/venv/bin/python monitor_sqlite.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target

```

#### Bước 3: Kích hoạt và chạy
```bash
sudo systemctl daemon-reload
sudo systemctl enable discord-bot
sudo systemctl start discord-bot
```

#### Quản lý bot:
- **Kiểm tra trạng thái**: `sudo systemctl status discord-bot`
- **Khởi động lại (sau khi sửa code)**: `sudo systemctl restart discord-bot`
- **Dừng bot**: `sudo systemctl stop discord-bot`
- **Xem log lỗi trực tiếp**: `journalctl -u discord-bot -f`

---

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


---

