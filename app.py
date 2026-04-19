#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "telethon>=1.36",
#   "fastapi>=0.115",
#   "uvicorn[standard]>=0.32",
#   "yt-dlp",
# ]
# ///
"""
TGrab — High-performance Telegram Media Downloader

Purpose:
  A unified streaming and downloading interface for Telegram media. Supports
  instant previews, mosaic gallery views, and batch background downloads.

Dependencies:
  - telethon (MTProto API)
  - fastapi (Backend framework)
  - uvicorn (ASGI server)
  - sqlite3 (Local metadata caching)

Usage:
  - via uv:     uv run app.py
  - via python: python app.py (ensure dependencies are installed)

Configuration:
  - Default Port: 7861
  - Downloads:    ~/Downloads/telegram (auto-created)
  - Cache:        cache.db (SQLite database storing channel & media metadata)
  - Auth:         tg_creds.json & tg.session (Telethon credentials/session)
"""

import asyncio, json, logging, os, re, sqlite3, subprocess, sys, uuid, webbrowser, mimetypes
from collections import OrderedDict

logger = logging.getLogger("tgrab")
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Optional, Any, Callable, Type, Iterable, Container, Union

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.middleware.gzip import GZipMiddleware
import uvicorn

from telethon import TelegramClient, errors, utils
from telethon.tl import types
from telethon.tl.types import (
    InputMessagesFilterPhotos,
    InputMessagesFilterVideo,
    InputMessagesFilterDocument,
)

# ── Imports ───────────────────────────────────────────────────────────────────
from config import PORT, CREDS_FILE, SESSION, DOWNLOADS, PREVIEWS, THUMBS_DIR
from db import _db_init, _db_run, _db_read, _db_get_channels, _db_upsert_channel, _db_get_media, _db_is_downloaded
from core import st, _mk_client, _client, _media_sse, _run_download, _run_ytdlp, \
    _safe_name, _msg_to_item, _hr_size, _media_type, _ext, _size


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    _db_init()
    THUMBS_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    PREVIEWS.mkdir(parents=True, exist_ok=True)
    if CREDS_FILE.exists() and SESSION.with_suffix(".session").exists():
        try:
            creds = json.loads(CREDS_FILE.read_text())
            st.api_id   = creds["api_id"]
            st.api_hash = creds["api_hash"]
            st.client = _mk_client(st.api_id, st.api_hash)
            await st.client.connect()
            if not await st.client.is_user_authorized():
                # Session file exists but is expired — clear it
                logger.warning("Saved session is not authorized — clearing")
                await st.client.disconnect()
                st.client = None
                SESSION.with_suffix(".session").unlink(missing_ok=True)
            else:
                logger.info("Restored Telegram session from disk")
        except Exception:
            # Transient network error: keep the client so _client() can reconnect
            logger.warning("Could not verify session on startup (network?); will retry on first request", exc_info=True)
    yield
    # Graceful shutdown: cancel all active downloads
    for evt in st.cancel_evts.values():
        evt.set()
    if st.client and st.client.is_connected():
        await st.client.disconnect()


# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI(lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=500)
app.mount("/static", StaticFiles(directory="static"), name="static")


# ── Auth ──────────────────────────────────────────────────────────────────────
class AuthInit(BaseModel):
    api_id: int
    api_hash: str
    phone: str

class AuthVerify(BaseModel):
    code: str
    password: Optional[str] = None


@app.get("/api/auth/status")
async def auth_status() -> dict[str, Any]:
    if not st.client:
        return {"authenticated": False, "phone": st.phone}
    try:
        if not st.client.is_connected():
            await st.client.connect()
        authed = await st.client.is_user_authorized()
    except Exception:
        authed = False
    return {"authenticated": authed, "phone": st.phone}


@app.get("/api/health")
async def health() -> dict[str, Any]:
    authed = bool(st.client and st.client.is_connected()
                  and await st.client.is_user_authorized())
    return {"status": "ok", "authenticated": authed}


@app.post("/api/auth/init")
async def auth_init(body: AuthInit) -> dict[str, Any]:
    if st.client and st.client.is_connected():
        await st.client.disconnect()
    st.api_id, st.api_hash, st.phone = body.api_id, body.api_hash, body.phone
    st.client = _mk_client(body.api_id, body.api_hash)
    try:
        await st.client.connect()
    except (errors.AuthKeyDuplicatedError, Exception) as e:
        # Session file is corrupted — delete and start fresh
        logger.warning(f"Session invalid ({e}), creating fresh session")
        SESSION.with_suffix(".session").unlink(missing_ok=True)
        st.client = _mk_client(body.api_id, body.api_hash)
        await st.client.connect()
    if await st.client.is_user_authorized():
        CREDS_FILE.write_text(json.dumps({"api_id": body.api_id, "api_hash": body.api_hash, "phone": body.phone}))
        return {"success": True, "already_authorized": True}
    try:
        result = await st.client.send_code_request(body.phone)
        st.phone_hash = result.phone_code_hash
        return {"success": True, "needs_otp": True}
    except errors.PhoneNumberInvalidError:
        raise HTTPException(400, "Invalid phone number. Include country code e.g. +919876543210")
    except errors.FloodWaitError as e:
        raise HTTPException(429, f"Too many attempts — wait {e.seconds}s before retrying")
    except errors.AuthRestartError:
        raise HTTPException(500, "Telegram auth restarted — please try again")
    except Exception as e:
        raise HTTPException(500, f"Telegram error: {e}")


@app.post("/api/auth/verify")
async def auth_verify(body: AuthVerify) -> dict[str, Any]:
    if not st.client:
        raise HTTPException(400, "Start auth first — go back and re-enter credentials")
    try:
        await st.client.sign_in(st.phone, body.code, phone_code_hash=st.phone_hash)
    except errors.SessionPasswordNeededError:
        if not body.password:
            return {"success": False, "needs_2fa": True}
        await st.client.sign_in(password=body.password)
    except errors.PhoneCodeInvalidError:
        raise HTTPException(400, "Invalid OTP code — double-check the code from Telegram")
    except errors.PhoneCodeExpiredError:
        raise HTTPException(400, "OTP expired — go back and request a new code")
    except errors.FloodWaitError as e:
        raise HTTPException(429, f"Too many attempts — wait {e.seconds}s")
    except Exception as e:
        raise HTTPException(500, f"Telegram error: {e}")
    # Save full creds including phone for session restore on next startup
    CREDS_FILE.write_text(json.dumps({"api_id": st.api_id, "api_hash": st.api_hash, "phone": st.phone}))
    return {"success": True}


@app.post("/api/auth/logout")
async def auth_logout() -> dict[str, Any]:
    if st.client:
        try:
            await st.client.log_out()
        except Exception:
            logger.warning("Failed to log out cleanly", exc_info=True)
        await st.client.disconnect()
        st.client = None
    SESSION.with_suffix(".session").unlink(missing_ok=True)
    CREDS_FILE.unlink(missing_ok=True)
    return {"success": True}


# ── Channels — SSE stream (channels appear one by one) ───────────────────────
@app.get("/api/channels")
async def stream_channels(request: Request):
    async def gen() -> AsyncGenerator[str, None]:
        c = await _client()

        # Fetch Telegram folder filters → build channel_id→folder_name mapping
        folder_map: dict[int, list[str]] = {}  # channel_id → [folder_names]
        folder_names: list[dict] = []
        try:
            from telethon.tl.functions.messages import GetDialogFiltersRequest
            result = await c(GetDialogFiltersRequest())
            filters = getattr(result, 'filters', result) if not isinstance(result, list) else result
            for f in filters:
                title = getattr(f, 'title', None)
                if not title:
                    continue
                # Handle both string titles and TextWithEntities
                if hasattr(title, 'text'):
                    title = title.text
                title = str(title)
                folder_names.append({"name": title, "emoji": getattr(f, 'emoticon', '') or ''})
                for peer in getattr(f, 'include_peers', []):
                    cid = getattr(peer, 'channel_id', None) or getattr(peer, 'chat_id', None)
                    if cid:
                        folder_map.setdefault(cid, []).append(title)
        except Exception:
            logger.debug("Could not fetch dialog filters", exc_info=True)

        # Emit folders list first
        yield f"data: {json.dumps({'folder_list': folder_names})}\n\n"

        async for d in c.iter_dialogs(limit=None):
            if await request.is_disconnected():
                break
            e = d.entity
            if not isinstance(e, (types.Channel, types.Chat)):
                continue
            
            # Use marked IDs (negative) to avoid Peer ID ambiguity and leverage Telethon's cache
            m_id = utils.get_peer_id(e)
            ch = {
                "id": m_id,
                "title": getattr(e, "title", "?"),
                "type": "channel" if isinstance(e, types.Channel) else "group",
                "members": getattr(e, "participants_count", None),
                "username": getattr(e, "username", None),
                "folders": folder_map.get(m_id, []) or folder_map.get(e.id, []), # Try both
            }
            await _db_run(lambda c=ch: _db_upsert_channel(c))
            yield f"data: {json.dumps(ch)}\n\n"
        yield 'data: {"done":true}\n\n'

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── Media SSE ─────────────────────────────────────────────────────────────────
@app.get("/api/media")
async def get_media(
    request: Request,
    channels: str = "",
    type: str = "all",
):
    ids = [int(x) for x in channels.split(",") if x.strip()]
    if not ids:
        raise HTTPException(400, "No channel IDs")
    return StreamingResponse(
        _media_sse(ids, type, request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/thumb/{channel_id}/{msg_id}")
async def get_thumb(channel_id: int, msg_id: int):
    key = f"{channel_id}_{msg_id}"
    t_path = THUMBS_DIR / f"{key}.jpg"
    
    # 1. RAM (O(1))
    data = st.thumbs.get(key)
    if data: return Response(data, media_type="image/jpeg", 
                            headers={"Cache-Control": "public, max-age=604800"})
    
    # 2. Disk (Persistent)
    if t_path.exists():
        data = t_path.read_bytes()
        st.thumbs.set(key, data)
        return Response(data, media_type="image/jpeg", 
                        headers={"Cache-Control": "public, max-age=604800"})
        
    # 3. Telegram — try stripped thumb first (no download, instant)
    try:
        c = await _client()
        entity = await c.get_entity(channel_id)
        msg = await c.get_messages(entity, ids=msg_id)

        # Try to extract stripped thumbnail bytes (embedded in metadata)
        thumb_bytes = None
        media = msg.media if msg else None
        if media:
            photo = getattr(media, 'photo', None) or getattr(media, 'document', None)
            if photo:
                for sz in getattr(photo, 'thumbs', []) or []:
                    if hasattr(sz, 'bytes') and sz.bytes:
                        # PhotoStrippedSize — reconstruct JPEG
                        raw = sz.bytes
                        if len(raw) > 0 and raw[0] == 1:
                            # Stripped format: needs JPEG header/footer
                            header = bytes([
                                0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46,
                                0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01,
                                0x00, 0x01, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43,
                                0x00, 0x28, 0x1C, 0x1E, 0x23, 0x1E, 0x19, 0x28,
                                0x23, 0x21, 0x23, 0x2D, 0x2B, 0x28, 0x30, 0x3C,
                                0x64, 0x41, 0x3C, 0x37, 0x37, 0x3C, 0x7B, 0x58,
                                0x5D, 0x49, 0x64, 0x91, 0x80, 0x99, 0x96, 0x8F,
                                0x80, 0x8C, 0x8A, 0xA0, 0xB4, 0xE6, 0xC3, 0xA0,
                                0xAA, 0xDA, 0xAD, 0x8A, 0x8C, 0xC8, 0xFF, 0xCB,
                                0xDA, 0xEE, 0xF5, 0xFF, 0xFF, 0xFF, 0x9B, 0xC1,
                                0xFF, 0xFF, 0xFF, 0xFA, 0xFF, 0xE6, 0xFD, 0xFF,
                                0xF8, 0xFF, 0xDB, 0x00, 0x43, 0x01, 0x2B, 0x2D,
                                0x2D, 0x3C, 0x35, 0x3C, 0x76, 0x41, 0x41, 0x76,
                                0xF8, 0xA5, 0x8C, 0xA5, 0xF8, 0xF8, 0xF8, 0xF8,
                                0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8,
                                0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8,
                                0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8,
                                0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8,
                                0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8,
                                0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8,
                            ])
                            footer = bytes([0xFF, 0xD9])
                            thumb_bytes = header + raw[3:] + footer
                        else:
                            thumb_bytes = raw
                        break

        if not thumb_bytes:
            # Fallback: download smallest thumb
            thumb_bytes = await c.download_media(msg, bytes, thumb=-1)

        if thumb_bytes:
            st.thumbs.set(key, thumb_bytes)
            t_path.write_bytes(thumb_bytes)
            return Response(thumb_bytes, media_type="image/jpeg", 
                            headers={"Cache-Control": "public, max-age=604800"})
    except Exception as e:
        logger.error(f"Failed to fetch thumbnail for {channel_id}_{msg_id}: {e}", exc_info=True)
    
    raise HTTPException(404, "Thumbnail unavailable")


# ── Preview (full file served as stream) ─────────────────────────────────────
@app.get("/api/preview/{channel_id}/{msg_id}")
async def get_preview(channel_id: int, msg_id: int, request: Request):
    """Serve media. Check DOWNLOADS first, then PREVIEWS cache, else fetch from TG and cache."""
    try:
        # 1. Check if already permanently downloaded
        local_path = await _db_run(lambda: _db_is_downloaded(channel_id, msg_id))
        if local_path and Path(local_path).exists():
            return FileResponse(local_path, headers={"Cache-Control": "public, max-age=86400"})

        # 2. Check preview disk cache
        key = f"{channel_id}_{msg_id}"
        for ext in (".jpg", ".mp4", ".png", ".webp"):
            p = PREVIEWS / f"{key}{ext}"
            if p.exists():
                return FileResponse(p, headers={"Cache-Control": "public, max-age=86400"})

        # 3. Fetch from TG
        c = await _client()
        entity = await c.get_entity(channel_id)
        msg = await c.get_messages(entity, ids=msg_id)
        mt = _media_type(msg)
        
        if mt is None:
            raise HTTPException(400, "Media cannot be previewed (unsupported or web link)")

        ext = _ext(msg)
        mime = mimetypes.guess_type("file" + ext)[0] or "application/octet-stream"
        
        # Override mimetypes for known safe types to ensure inline display
        if mt == "photo" or ext in (".jpg", ".jpeg", ".png", ".webp"):
            mime = "image/jpeg" if ext in (".jpg", ".jpeg") else f"image/{ext[1:]}"
        elif mt == "video" or ext in (".mp4", ".gif"):
            mime = "video/mp4"

        cache_path = PREVIEWS / f"{key}{ext}"

        if mt == "photo":
            data = await c.download_media(msg, bytes)
            if data:
                with open(cache_path, "wb") as f: f.write(data)
            return Response(data, media_type=mime, headers={"Cache-Control": "public, max-age=86400"})
        
        async def _stream() -> AsyncGenerator[bytes, None]:
            # Use msg instead of msg.media to provide context for protected content
            async for chunk in c.iter_download(msg, request_size=128*1024):
                if await request.is_disconnected():
                    break
                yield chunk

        return StreamingResponse(_stream(), media_type=mime, 
                                 headers={"Cache-Control": "public, max-age=86400"})
    except Exception as e:
        raise HTTPException(500, str(e))


# ── Browser Download (file streams to user's device) ─────────────────────────
@app.get("/api/file/{channel_id}/{msg_id}")
async def get_file(channel_id: int, msg_id: int, request: Request):
    """Stream a file from Telegram directly to the user's browser for local save."""
    try:
        c = await _client()
        entity = await c.get_entity(channel_id)
        msg = await c.get_messages(entity, ids=msg_id)
        if not msg or not msg.media:
            raise HTTPException(404, "Media not found")

        mt = _media_type(msg)
        if mt is None:
            raise HTTPException(400, "Unsupported media type")

        ext = _ext(msg)
        # Build filename from document attributes or fallback
        fname = f"{msg_id}{ext}"
        doc = getattr(msg, "document", None)
        if doc:
            for a in doc.attributes:
                if hasattr(a, "file_name") and a.file_name:
                    fname = a.file_name
                    break

        mime = mimetypes.guess_type("file" + ext)[0] or "application/octet-stream"
        size = _size(msg) if hasattr(msg, 'document') or hasattr(msg, 'photo') else 0

        if mt == "photo":
            data = await c.download_media(msg, bytes)
            if not data:
                raise HTTPException(500, "Failed to download photo")
            headers = {
                "Content-Disposition": f'attachment; filename="{fname}"',
                "Content-Length": str(len(data)),
            }
            return Response(data, media_type=mime, headers=headers)

        # For videos/documents, stream chunk by chunk
        async def _stream() -> AsyncGenerator[bytes, None]:
            # Use msg instead of msg.media to provide context for protected content
            async for chunk in c.iter_download(msg, request_size=256 * 1024):
                if await request.is_disconnected():
                    break
                yield chunk

        headers = {
            "Content-Disposition": f'attachment; filename="{fname}"',
        }
        if size:
            headers["Content-Length"] = str(size)

        return StreamingResponse(_stream(), media_type=mime, headers=headers)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


# ── Server-side Download (legacy) ────────────────────────────────────────────
class DownloadReq(BaseModel):
    items: list[dict]


@app.post("/api/download")
async def start_download(body: DownloadReq) -> dict[str, str]:
    job_id = str(uuid.uuid4())
    st.jobs[job_id] = {"status": "queued", "total": 0, "done": 0,
                       "pct": 0, "current": None, "files": [], "errors": []}
    st.cancel_evts[job_id] = asyncio.Event()
    asyncio.create_task(_run_download(job_id, body.items))
    return {"job_id": job_id}


@app.post("/api/download/{job_id}/cancel")
async def cancel_download(job_id: str) -> dict[str, bool]:
    ev = st.cancel_evts.get(job_id)
    if ev:
        ev.set()
        return {"cancelled": True}
    return {"cancelled": False}


@app.get("/api/download/{job_id}/progress")
async def download_progress(job_id: str, request: Request):
    if job_id not in st.jobs:
        raise HTTPException(404, "Job not found")

    async def _stream() -> AsyncGenerator[str, None]:
        while True:
            if await request.is_disconnected():
                break
            job = st.jobs[job_id]
            yield f"data: {json.dumps(job)}\n\n"
            if job["status"] in ("done", "cancelled", "error"):
                break
            await asyncio.sleep(0.4)

    return StreamingResponse(_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── yt-dlp ────────────────────────────────────────────────────────────────────
class YtReq(BaseModel):
    url: str
    fmt: str = "video"   # "video" or "audio"


@app.post("/api/ytdlp")
async def start_ytdlp(body: YtReq) -> dict[str, str]:
    job_id = str(uuid.uuid4())
    st.jobs[job_id] = {"status": "queued", "pct": 0, "current": None, "url": body.url}
    st.cancel_evts[job_id] = asyncio.Event()
    asyncio.create_task(_run_ytdlp(job_id, body.url, body.fmt))
    return {"job_id": job_id}


# ── Gallery data API ─────────────────────────────────────────────────────────
@app.get("/api/gallery-data")
async def gallery_data(channels: str = "") -> list[dict[str, Any]]:
    """Return cached media items for the gallery, with thumb/preview URLs."""
    if channels:
        ids = [int(x) for x in channels.split(",") if x.strip()]
    else:
        chs = await _db_read(_db_get_channels)
        ids = [ch["id"] for ch in chs]

    result = []
    for cid in ids:
        rows = await _db_read(lambda c=cid: _db_get_media(c, "all"))
        for row in rows:
            t = row["type"]
            result.append({
                "type":       t if t in ("photo", "video") else "document",
                "thumb":      f"/api/thumb/{cid}/{row['msg_id']}",
                "preview":    f"/api/preview/{cid}/{row['msg_id']}",
                "title":      row["filename"],
                "sub":        row.get("date", "") or "",
                "caption":    row.get("caption", "") or "",
                "channel_id": cid,
                "msg_id":     row["msg_id"],
                "size":       row.get("size", 0),
            })
    return result


# ── Root: serve the SPA ───────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index() -> FileResponse:
    return FileResponse("static/index.html")


# ── Gallery page ──────────────────────────────────────────────────────────────
@app.get("/gallery", response_class=HTMLResponse)
async def gallery() -> FileResponse:
    return FileResponse("static/gallery.html")



if __name__ == "__main__":
    async def _run():
        config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="warning")
        server = uvicorn.Server(config)

        async def _open():
            await asyncio.sleep(1.4)
            webbrowser.open(f"http://localhost:{PORT}")

        asyncio.create_task(_open())
        await server.serve()

    asyncio.run(_run())
