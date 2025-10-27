# 🧠 Vanna Project

---

## 🚀 Hướng dẫn cài đặt

### 1. Tạo và kích hoạt môi trường ảo (Windows)

```bash
python -m venv vanna_env
vanna_env\Scripts\activate
```

**Linux/macOS:**

```bash
python3 -m venv vanna_env
source vanna_env/bin/activate
```

---

### 2. Cập nhật công cụ và cài đặt thư viện cần thiết

**Lưu ý đã có Ollama**

Truy cập trang chính thức:
👉 https://ollama.com/download

```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
ollama pull mistral
```

Liệt kê các model đã tải:

```bash
ollama list
```

---

## ▶️ Chạy chương trình

Sau khi hoàn tất cài đặt, khởi chạy ứng dụng bằng lệnh:

```bash
python vanna_ai.py
```

Mở trình duyệt và truy cập địa chỉ sau để bắt đầu sử dụng:

```
http://localhost:8000
```

---
