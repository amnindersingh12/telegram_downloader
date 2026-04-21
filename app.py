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

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
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
from db import _db_init, _db_run, _db_read, _db_get_channels, _db_upsert_channel, _db_get_media, _db_is_downloaded, _db_get_mirrors, _db_get_sync_rules, _db_add_sync_rule, _db_remove_sync_rule
from core import st, _mk_client, _client, _media_sse, _run_download, _run_ytdlp, _run_mirror, \
    _safe_name, _msg_to_item, _hr_size, _media_type, _ext, _size, _get_entity_robust, _restore_jobs, \
    _fetch_thumb, _thumb_worker


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
                # Initialize event handlers and sync rules proactively
                from core import _on_new_message
                st.client.add_event_handler(_on_new_message)
                
                # Load sync rules
                rules = await _db_run(_db_get_sync_rules)
                for r in rules:
                    sid, tid = r["source_id"], r["target_id"]
                    if sid not in st.sync_map: st.sync_map[sid] = set()
                    st.sync_map[sid].add(tid)
                    logger.info(f"Auto-Sync active: {sid} -> {tid}")
        except Exception:
            # Transient network error: keep the client so _client() can reconnect
            logger.warning("Could not verify session on startup (network?); will retry on first request", exc_info=True)
    
    await _restore_jobs()
    # Parallel thumbnail workers for high-speed pre-fetching
    for _ in range(4):
        asyncio.create_task(_thumb_worker())
    asyncio.create_task(_cleanup_previews_worker())
    yield
    # Graceful shutdown: cancel all active downloads
    for evt in st.cancel_evts.values():
        evt.set()
    
    # Wait briefly for thumb_queue to finish pending tasks
    if not st.thumb_queue.empty():
        logger.info(f"Shutdown: waiting for {st.thumb_queue.qsize()} thumbs...")
        try: await asyncio.wait_for(st.thumb_queue.join(), timeout=3.0)
        except asyncio.TimeoutError: pass

    if st.client and st.client.is_connected():
        await st.client.disconnect()

async def _cleanup_previews_worker():
    """Background worker that periodically limits the size of the previews/ directory."""
    while True:
        try:
            from config import PREVIEWS
            if not PREVIEWS.exists(): continue
            
            # Max 10GB of previews
            MAX_SIZE = 10 * 1024 * 1024 * 1024
            files = []
            total_size = 0
            for f in PREVIEWS.glob("*"):
                if f.is_file():
                    stat = f.stat()
                    files.append((stat.st_mtime, stat.st_size, f))
                    total_size += stat.st_size
            
            if total_size > MAX_SIZE:
                # Sort by mtime (oldest first)
                files.sort()
                to_delete = total_size - (MAX_SIZE * 0.8) # Keep 80% of limit
                deleted = 0
                for mtime, size, f in files:
                    if deleted >= to_delete: break
                    f.unlink(missing_ok=True)
                    deleted += size
                logger.info(f"Cleaned up {deleted // (1024*1024)}MB of old previews")
        except Exception as e:
            logger.error(f"Preview cleanup worker failed: {e}")
        
        await asyncio.sleep(3600) # Check every hour


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


@app.get("/api/health")
async def get_health():
    return {
        "status": "ok",
        "thumb_queue": st.thumb_queue.qsize(),
        "jobs": len(st.jobs)
    }

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
                if hasattr(title, 'text'):
                    title = title.text
                title = str(title)
                
                # USER REQUEST: Remove "Personal" folder
                if title.lower() == "personal":
                    continue
                    
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
            
            m_id = utils.get_peer_id(e)
            
            is_creator = getattr(e, "creator", False)
            admin_rights = getattr(e, "admin_rights", None)
            is_admin = bool(admin_rights and admin_rights.post_messages) if admin_rights else is_creator

            ch = {
                "id": m_id,
                "title": getattr(e, "title", "?"),
                "type": "channel" if isinstance(e, types.Channel) else "group",
                "members": getattr(e, "participants_count", None),
                "unread": d.unread_count,  # Unified unread count
                "username": getattr(e, "username", None),
                "folders": folder_map.get(m_id, []) or folder_map.get(e.id, []), 
                "is_creator": is_creator,
                "can_post": is_admin or is_creator,
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
    thumb_bytes = await _fetch_thumb(channel_id, msg_id)
    if thumb_bytes:
        return Response(thumb_bytes, media_type="image/jpeg", 
                        headers={"Cache-Control": "public, max-age=31536000, immutable"})
    
    raise HTTPException(404, "Thumbnail unavailable")


# ── Preview (full file served as stream) ─────────────────────────────────────
@app.get("/api/preview/{channel_id}/{msg_id}")
async def get_preview(channel_id: int, msg_id: int, request: Request):
    """Serve media. Check DOWNLOADS first, then PREVIEWS cache, else fetch from TG and cache."""
    try:
        # 1. Check if already permanently downloaded
        local_path = await _db_run(lambda: _db_is_downloaded(channel_id, msg_id))
        if local_path and Path(local_path).exists():
            return FileResponse(local_path, headers={"Cache-Control": "public, max-age=31536000, immutable"})

        # 2. Check preview disk cache
        key = f"{channel_id}_{msg_id}"
        for ext in (".jpg", ".mp4", ".png", ".webp"):
            p = PREVIEWS / f"{key}{ext}"
            if p.exists():
                return FileResponse(p, headers={"Cache-Control": "public, max-age=31536000, immutable"})

        # 3. Fetch from TG
        c = await _client()
        entity = await _get_entity_robust(c, channel_id)
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
            return Response(data, media_type=mime, headers={"Cache-Control": "public, max-age=31536000, immutable"})
        
        async def _stream() -> AsyncGenerator[bytes, None]:
            # Use msg instead of msg.media to provide context for protected content
            async for chunk in c.iter_download(msg, request_size=128*1024):
                if await request.is_disconnected():
                    break
                yield chunk

        return StreamingResponse(_stream(), media_type=mime, 
                                 headers={"Cache-Control": "public, max-age=31536000, immutable"})
    except Exception as e:
        raise HTTPException(500, str(e))


# ── Browser Download (file streams to user's device) ─────────────────────────
@app.get("/api/file/{channel_id}/{msg_id}")
async def get_file(channel_id: int, msg_id: int, request: Request):
    """Stream a file from Telegram directly to the user's browser for local save."""
    try:
        c = await _client()
        entity = await _get_entity_robust(c, channel_id)
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
    st.tasks[job_id] = asyncio.create_task(_run_download(job_id, body.items))
    return {"job_id": job_id}


@app.post("/api/download/{job_id}/cancel")
async def cancel_download(job_id: str) -> dict[str, bool]:
    # Set event for loop-level checks
    if job_id in st.cancel_evts:
        st.cancel_evts[job_id].set()
    # Cancel task for immediate I/O abort
    if job_id in st.tasks:
        st.tasks[job_id].cancel()
        if job_id in st.jobs:
            st.jobs[job_id]["status"] = "cancelled"
        return {"cancelled": True}
    return {"cancelled": False}


@app.post("/api/jobs/cancel-all")
async def cancel_all_jobs():
    count = 0
    for jid, ev in list(st.cancel_evts.items()):
        ev.set()
        count += 1
    for jid, task in list(st.tasks.items()):
        task.cancel()
    return {"cancelled": count}


@app.post("/api/jobs/clear-done")
async def clear_done_jobs():
    cleared = 0
    # Create a list of keys to avoid modification during iteration
    for jid in list(st.jobs.keys()):
        job = st.jobs.get(jid)
        if job and job.get("status") in ("done", "cancelled", "error"):
            st.jobs.pop(jid, None)
            st.cancel_evts.pop(jid, None)
            cleared += 1
    return {"cleared": cleared}


@app.get("/api/download/{job_id}/progress")
async def download_progress(job_id: str, request: Request):
    if job_id not in st.jobs:
        raise HTTPException(404, "Job not found")

    async def _stream() -> AsyncGenerator[str, None]:
        while True:
            if await request.is_disconnected():
                break
            job = st.jobs[job_id]
            # Copy to avoid mutation issues during JSON serialization
            data = {
                "status": job["status"],
                "pct": job["pct"],
                "done": job.get("done", 0),
                "total": job.get("total", 0),
                "current": job.get("current"),
                "logs": job.get("logs", [])
            }
            yield f"data: {json.dumps(data)}\n\n"
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
    st.tasks[job_id] = asyncio.create_task(_run_ytdlp(job_id, body.url, body.fmt))
    return {"job_id": job_id}


# ── Channel Mirroring ────────────────────────────────────────────────────────
class MirrorReq(BaseModel):
    source_id: Union[int, str]
    target_id: Union[int, str]
    limit: Optional[int] = None


@app.post("/api/mirror/start")
async def start_mirror(body: MirrorReq) -> dict[str, str]:
    # 1. Prevent duplicate jobs for same pair
    for jid, j in st.jobs.items():
        if j.get("type") == "mirror" and j.get("status") in ("running", "queued"):
            if str(j.get("source_id")) == str(body.source_id) and str(j.get("target_id")) == str(body.target_id):
                logger.info(f"Ignoring duplicate mirror request for {body.source_id} -> {body.target_id}")
                return {"job_id": jid, "status": "already_running"}

    job_id = f"mirror_{uuid.uuid4().hex[:8]}"
    st.jobs[job_id] = {
        "id": job_id, "source_id": body.source_id, "target_id": body.target_id,
        "status": "queued", "type": "mirror", "pct": 0, "current": None, 
        "logs": ["Queued for execution..."]
    }
    st.cancel_evts[job_id] = asyncio.Event()
    st.tasks[job_id] = asyncio.create_task(_run_mirror(job_id, body.source_id, body.target_id, body.limit))
    return {"job_id": job_id}


@app.get("/api/mirrors")
async def get_mirrors():
    # Return merged live memory state for accuracy on refresh
    # We prioritize st.jobs (live) over DB rows
    db_rows = await _db_run(_db_get_mirrors)
    # Combine - though st.jobs handles everything after restoration
    # Simplest: just return st.jobs filtered for mirror types
    mirrors = [j for j in st.jobs.values() if j.get("type") == "mirror" or j.get("id", "").startswith("mirror_")]
    # Sort: running first, then queued, then others. Within status, sort by ID descending (newest first).
    def sort_key(j):
        status_order = {"running": 0, "queued": 1, "done": 2, "error": 3, "cancelled": 4}
        # Use str(j.get("id")) for lexicographical descending sort as a secondary key
        return (status_order.get(j.get("status"), 99), -int(j.get("id", "0").split("_")[-1]) if "_" in str(j.get("id", "")) else 0)
    
    # Actually, simpler sort for now: status priority, then ID descending
    mirrors.sort(key=lambda j: (
        0 if j.get("status") == "running" else (1 if j.get("status") == "queued" else 2),
        -(int(j["id"].split("_")[1], 16) if "_" in j.get("id", "") else 0)
    ))
    return mirrors


@app.post("/api/mirror/sync/start")
async def start_sync(body: MirrorReq):
    c = await _client()
    # Resolve IDs to integers for the sync_map
    src = await _get_entity_robust(c, body.source_id)
    dst = await _get_entity_robust(c, body.target_id)
    sid, tid = src.id, dst.id
    # Marked IDs are better for internal comparison
    from telethon import utils
    sid, tid = utils.get_peer_id(src), utils.get_peer_id(dst)

    if sid not in st.sync_map: st.sync_map[sid] = set()
    st.sync_map[sid].add(tid)
    await _db_run(lambda: _db_add_sync_rule(sid, tid))
    return {"status": "ok", "source": sid, "target": tid}


@app.post("/api/mirror/sync/stop")
async def stop_sync(body: MirrorReq):
    c = await _client()
    src = await _get_entity_robust(c, body.source_id)
    dst = await _get_entity_robust(c, body.target_id)
    from telethon import utils
    sid, tid = utils.get_peer_id(src), utils.get_peer_id(dst)

    if sid in st.sync_map:
        st.sync_map[sid].discard(tid)
        if not st.sync_map[sid]: del st.sync_map[sid]
    await _db_run(lambda: _db_remove_sync_rule(sid, tid))
    return {"status": "ok"}


@app.get("/api/mirror/sync/list")
async def list_sync_rules():
    rules = await _db_run(_db_get_sync_rules)
    from collections import defaultdict
    grouped = defaultdict(list)
    for r in rules:
        grouped[r["source_id"]].append(r["target_id"])
    return [{"source_id": s, "targets": t} for s, t in grouped.items()]


@app.post("/api/jobs/reset")
async def reset_activity():
    # Cancel all running tasks
    for ev in list(st.cancel_evts.values()):
        ev.set()
    
    # Reset in-memory jobs
    st.jobs.clear()
    st.jobs["sync_activity"] = {
        "status": "running", "type": "sync", 
        "logs": [f"{datetime.now().strftime('%H:%M:%S')} System: Activity & Logs reset by user."], 
        "pct": 100
    }
    st.sync_map.clear()
    
    # Wipe relevant DB tables
    def _wipe():
        from db import _db_connect
        with _db_connect() as c:
            c.execute("DELETE FROM mirrors")
            c.execute("DELETE FROM mirrored_messages")
            c.execute("DELETE FROM sync_rules")
            c.execute("DELETE FROM downloads") # Optional: user said clean logs, sometimes includes history
    
    await _db_run(_wipe)
    return {"status": "ok"}


# ── Activity Sync / Live Updates ──────────────────────────────────────────
@app.get("/api/updates")
async def stream_updates(request: Request):
    async def gen() -> AsyncGenerator[str, None]:
        q = asyncio.Queue()
        st.queues.add(q)
        try:
            while True:
                if await request.is_disconnected(): break
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {json.dumps(ev)}\n\n"
                except asyncio.TimeoutError:
                    yield "comment: ping\n\n"
        finally:
            st.queues.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── Gallery data API ─────────────────────────────────────────────────────────
@app.get("/api/gallery-data")
async def gallery_data(channels: str = "", offset: int = 0, limit: int = 100) -> list[dict[str, Any]]:
    """Return cached media items for the gallery, with pagination."""
    if channels:
        ids = [int(x) for x in channels.split(",") if x.strip()]
    else:
        chs = await _db_read(_db_get_channels)
        ids = [ch["id"] for ch in chs]

    result = []
    # For simplicity when multiple channels are selected, we fetch from all
    # In a perfect world, we'd have a single SQL query for all IDs
    for cid in ids:
        # Note: This is still a bit heavy for 42 channels if we don't limit per channel
        # or use a global join. For now, let's limit the total result.
        if len(result) >= limit: break
        rows = await _db_read(lambda c=cid: _db_get_media(c, "all", limit=limit, offset=offset))
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
            # Trigger background prefetch for missing thumbs only
            t_path = THUMBS_DIR / f"{cid}_{row['msg_id']}.jpg"
            if not t_path.exists():
                st.thumb_queue.put_nowait((cid, row["msg_id"]))
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
        config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info")
        server = uvicorn.Server(config)

        async def _open():
            await asyncio.sleep(1.4)
            webbrowser.open(f"http://localhost:{PORT}")

        asyncio.create_task(_open())
        await server.serve()

    asyncio.run(_run())
