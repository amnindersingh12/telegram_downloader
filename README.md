# 🛰️ TGrab — High-Performance Telegram Media Downloader

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A unified streaming and downloading interface for Telegram. TGrab allows you to browse, preview, and batch download media from any channel or group with zero friction.

---

## ✨ Features

- **🚀 Instant Cataloging**: Lists all your joined channels and groups immediately.
- **🖼️ Real-time Streaming**: Media items stream into the UI as they are discovered—no waiting for full scans.
- **⚡ Virtual Layout**: Handles 100,000+ items smoothly using a custom JS-based virtualized grid.
- **🎬 Smart Previews**: Hover to play videos, instant lightbox for photos, and lazy-loaded thumbnails.
- **📂 Automatic Organization**: Downloads are saved to `~/Downloads/telegram/<channel_name>/`.
- **🔄 Resume Support**: Skips already downloaded files and resumes interrupted jobs.
- **📦 Zero Config**: Auth once via OTP, and the session is cached securely in `tg.session`.

---

## 🚀 Quick Start

### Option A: Using `uv` (Recommended)
The fastest way to run TGrab without managing environments.
```zsh
uv run app.py
```

### Option B: Using standard Python
```zsh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

Once running, open **[http://localhost:7861](http://localhost:7861)** in your browser.

---

## 🔑 First Launch Setup

1. Get your **API ID** and **API Hash** from [my.telegram.org](https://my.telegram.org).
2. Launch TGrab and enter your credentials.
3. Enter your **phone number** (with country code, e.g., `+1234567890`).
4. Enter the **OTP** sent by Telegram.
5. You're in! Credentials and session are stored locally and won't be requested again.

---

## 📁 Project Structure

| File | Description |
|---|---|
| `app.py` | FastAPI backend & API routing. |
| `core.py` | Telethon logic & media streaming engine. |
| `db.py` | SQLite persistence for metadata & history. |
| `config.py` | App-wide constants and path settings. |
| `static/` | Vanilla JS/CSS frontend (Zero dependencies). |

---

## 🛡️ Privacy & Security

- **Local-first**: All session data, API keys, and media metadata stay on your machine.
- **No Cloud**: TGrab does not send your data to any any server besides Telegram's official MTProto endpoints.
- **Open Source**: Audit the code yourself—it's less than 2k lines of Python.

---

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.
