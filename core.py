import asyncio, base64, json, logging, os, re, sys
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Optional, Any
from fastapi import HTTPException, Request
from telethon.tl import types
from telethon.tl.types import (
    InputMessagesFilterPhotos, InputMessagesFilterVideo, InputMessagesFilterDocument
)
from config import SESSION, DOWNLOADS, THUMB_CACHE_SIZE, MAX_CONCURRENT_DOWNLOADS, JOB_TTL_SECONDS, THUMBS_DIR
from db import _db_run, _db_read, _db_get_media, _db_last_msg_id, _db_cache_media, \
    _db_is_downloaded, _db_mark_downloaded, _db_cache_media_batch, _db_upsert_mirror, \
    _db_get_mirrors, _db_is_mirrored, _db_add_mirror_mapping, _db_get_sync_rules, _db_add_sync_rule, _db_remove_sync_rule
from telethon import TelegramClient, errors, events, utils

logger = logging.getLogger("tgrab")


def _is_file_ref_error(exc: Exception) -> bool:
    """Check if an exception is a Telegram file reference expiry/invalid error.
    Handles both the specific error class (Telethon >= 1.24) and string fallback."""
    for cls_name in ('FileReferenceExpiredError', 'FileReferenceInvalidError'):
        cls = getattr(errors, cls_name, None)
        if cls and isinstance(exc, cls):
            return True
    msg = str(exc).lower()
    return 'file reference' in msg and ('expired' in msg or 'invalid' in msg)


def _msg_fingerprint(msg) -> Optional[str]:
    """Generate a content fingerprint for deduplication.
    Uses Telegram's internal media IDs for media, text hash for text-only."""
    if not msg:
        return None
    # Media fingerprint: Telegram's unique media ID (survives forwards/copies)
    photo = getattr(msg, 'photo', None)
    if photo and hasattr(photo, 'id'):
        return f"photo_{photo.id}"
    doc = getattr(msg, 'document', None)
    if doc and hasattr(doc, 'id'):
        return f"doc_{doc.id}"
    # Text-only: hash the content
    text = (getattr(msg, 'message', None) or "").strip()
    if text:
        import hashlib
        return f"text_{hashlib.md5(text.encode()).hexdigest()}"
    return None


# ── LRU thumb cache ───────────────────────────────────────────────────────────
class LRU:
    def __init__(self, maxsize: int = THUMB_CACHE_SIZE):
        self._d: OrderedDict[str, bytes] = OrderedDict()
        self._max = maxsize

    def get(self, k: str) -> Optional[bytes]:
        if k in self._d:
            self._d.move_to_end(k)
            return self._d[k]
        return None

    def set(self, k: str, v: bytes) -> None:
        if k in self._d:
            self._d.move_to_end(k)
        else:
            if len(self._d) >= self._max:
                self._d.popitem(last=False)
        self._d[k] = v


class _State:
    def __init__(self):
        self.api_id:      Optional[int] = None
        self.api_hash:    Optional[str] = None
        self.phone:       Optional[str] = None
        self.phone_hash:  Optional[str] = None
        self.client:      Optional[TelegramClient] = None
        self.queues:      set[asyncio.Queue] = set()
        self.jobs:   dict[str, Any] = {
            "sync_activity": {"status": "running", "type": "sync", "logs": ["Live Sync service started..."], "pct": 100}
        }
        self.cancel_evts: dict[str, asyncio.Event] = {}
        self.tasks: dict[str, asyncio.Task] = {} # Active job tasks for hard cancellation
        self.sync_map: dict[int, set[int]] = {} # source_id -> {target_ids}
        self.thumbs:   LRU = LRU(THUMB_CACHE_SIZE)
        self.thumb_queue: asyncio.Queue = asyncio.Queue()
        self.entity_cache: dict[int, Any] = {}  # channel_id -> entity object

st = _State()


@events.register(events.NewMessage)
async def _on_new_message(event):
    src_id = event.chat_id
    m = event.message
    
    # ── 1. Live Sync / Mirroring ───────────────────────────────────────────
    if src_id in st.sync_map:
        targets = st.sync_map[src_id]
        for dst_id in targets:
            try:
                if await _db_run(lambda d=dst_id: _db_is_mirrored(src_id, m.id, d)): continue
                try:
                    try:
                        await event.client.send_message(dst_id, m, comment_to=None)
                    except Exception as _fre:
                        if not _is_file_ref_error(_fre): raise
                        # Re-fetch message for fresh file references
                        m = await event.client.get_messages(src_id, ids=m.id)
                        if not m: continue
                        await event.client.send_message(dst_id, m, comment_to=None)
                except errors.ChatForwardsRestrictedError:
                    if m.media:
                        from io import BytesIO
                        import mimetypes
                        bio = BytesIO()
                        await event.client.download_media(m, bio)
                        bio.seek(0)
                        fn = m.file.name
                        doc = getattr(m.media, 'document', None)
                        if not fn and doc and doc.mime_type:
                            ex = mimetypes.guess_extension(doc.mime_type)
                            if ex: fn = f"sync_{m.id}{ex}"
                        bio.name = fn or f"sync_{m.id}"
                        await event.client.send_file(dst_id, bio, caption=m.message, formatting_entities=m.entities, attributes=doc.attributes if doc else [])
                    else:
                        await event.client.send_message(dst_id, m.message, formatting_entities=m.entities)
                
                await _db_run(lambda d=dst_id: _db_add_mirror_mapping(src_id, m.id, d))
                log_msg = f"{datetime.now().strftime('%H:%M:%S')} [LIVE] Sync: {src_id} ➜ {dst_id}"
                st.jobs["sync_activity"]["logs"] = (st.jobs["sync_activity"].get("logs", []) + [log_msg])[-500:]
            except Exception as e:
                logger.error(f"Live Sync Error: {e}")

    # ── 2. Notify connected clients (Unified Updates) ──────────────────────
    item = _msg_to_item(m, src_id)
    # Background cache it
    if item:
        asyncio.create_task(_db_run(lambda: _db_cache_media_batch([item])))
        # Also pre-fetch thumb
        st.thumb_queue.put_nowait((src_id, m.id))

    for q in st.queues:
        q.put_nowait({
            "type": "new_message",
            "channel_id": src_id,
            "msg_id": m.id,
            "item": item
        })


def _mk_client(api_id: int, api_hash: str) -> TelegramClient:
    return TelegramClient(str(SESSION), api_id, api_hash, 
                          device_model="Desktop", 
                          system_version="Linux",
                          app_version="TGrab 1.0")


async def _client() -> TelegramClient:
    if not st.client:
        raise HTTPException(503, "Not authenticated")

    if not st.client.is_connected():
        try:
            await st.client.connect()
            if not await st.client.is_user_authorized():
                raise HTTPException(503, "Session expired — please re-login")
            
            # Re-register event handler on reconnect
            st.client.add_event_handler(_on_new_message)
            
            # Load sync rules if never loaded
            if not st.sync_map:
                rules = await _db_run(_db_get_sync_rules)
                for r in rules:
                    sid, tid = r["source_id"], r["target_id"]
                    if sid not in st.sync_map: st.sync_map[sid] = set()
                    st.sync_map[sid].add(tid)
                    logger.info(f"Auto-Sync enabled: {sid} -> {tid}")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(503, "Failed to reconnect to Telegram")
    return st.client


async def _get_entity_robust(client: TelegramClient, peer: Any) -> Any:
    """Try to get entity. If ID-based lookups fail, sweep dialogs to find and cache it."""
    # Normalize ID-like strings to integers
    if isinstance(peer, str):
        clean = peer.lstrip('-')
        if clean.isdigit():
            peer = int(peer)

    # 1. Memory Cache
    if isinstance(peer, (int, float)) and int(peer) in st.entity_cache:
        return st.entity_cache[int(peer)]
    
    try:
        ent = await client.get_entity(peer)
        if hasattr(ent, "id"): st.entity_cache[ent.id] = ent
        return ent
    except Exception:
        # Only sweep if peer looks like an ID
        is_id = False
        pid = 0
        if isinstance(peer, (int, float)):
            is_id = True; pid = int(peer)
        
        if is_id:
            # Prevent repetitive sweeps for known missing IDs
            now = datetime.now().timestamp()
            last_sweep = getattr(st, "_last_sweep", 0)
            if now - last_sweep < 60: # Max one sweep per minute
                logger.warning(f"Entity {pid} not found; skipping sweep (too soon)")
                raise errors.PeerIdInvalidError(None)

            logger.info(f"Entity {pid} not in cache — sweeping recent dialogs...")
            count = 0
            async for d in client.iter_dialogs(limit=500):
                st.entity_cache[d.id] = d.entity
                if d.id == pid:
                    st._last_sweep = datetime.now().timestamp() # Successful sweep
                    return d.entity
                count += 1
                if count % 100 == 0:
                    logger.info(f"Swept {count} dialogs...")
            
            st._last_sweep = datetime.now().timestamp() # Failed sweep
            logger.warning(f"Entity {pid} not found in 500 dialogs.")
        raise errors.PeerIdInvalidError(None)


async def _iter_with_retry(client, entity, max_retries=3, **kwargs):
    """Wrap iter_messages with retry + exponential backoff for Telegram timeouts."""
    retries = 0
    while True:
        try:
            async for msg in client.iter_messages(entity, **kwargs):
                yield msg
                retries = 0
            return
        except errors.FloodWaitError as e:
            retries += 1
            if retries > max_retries:
                raise
            logger.warning(f"FloodWait retry {retries}/{max_retries}, waiting {e.seconds}s")
            await asyncio.sleep(e.seconds)
        except (TimeoutError, ConnectionError, OSError) as e:
            retries += 1
            if retries > max_retries:
                raise
            wait = min(2 ** retries, 30)
            logger.warning(f"Timeout retry {retries}/{max_retries}, waiting {wait}s: {e}")
            await asyncio.sleep(wait)


def _media_type(msg: Any) -> Optional[str]:
    # Ignore web pages which cause iter_download crashes
    if isinstance(getattr(msg, "media", None), types.MessageMediaWebPage):
        return None

    if getattr(msg, "photo", None):
        return "photo"
    doc = getattr(msg, "document", None)
    if doc:
        mime = getattr(doc, "mime_type", "") or ""
        if mime.startswith("video") or mime == "image/gif":
            return "video"
        if mime.startswith("image"):
            return "photo"
        return "document"
    if getattr(msg, "video", None):
        return "video"
    return None


def _ext(msg: Any) -> str:
    # First try to extract exact file extension from document attributes
    doc = getattr(msg, "document", None)
    if doc:
        for a in doc.attributes:
            if hasattr(a, "file_name") and a.file_name:
                ext = Path(a.file_name).suffix.lower()
                if ext: return ext
    
    # Fallback to general type mappings
    t = _media_type(msg)
    if t == "photo": return ".jpg"
    if t == "video": return ".mp4"
    return ".bin"


def _size(msg: Any) -> int:
    doc = getattr(msg, "document", None)
    if doc: return getattr(doc, "size", 0)
    photo = getattr(msg, "photo", None)
    if photo:
        sizes = [s for s in getattr(photo, "sizes", []) if hasattr(s, "size")]
        return sizes[-1].size if sizes else 0
    return 0


def _hr_size(n: int) -> str:
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024: return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"


def _get_stripped_thumb(msg: Any) -> Optional[str]:
    """Extract and convert Telegram's PhotoStrippedSize to a usable base64 JPG."""
    from telethon.utils import stripped_photo_to_jpg
    media = getattr(msg, "media", None)
    if not media: return None
    
    photo = getattr(msg, "photo", None)
    doc = getattr(msg, "document", None)
    
    try:
        if photo:
            for size in photo.sizes:
                if isinstance(size, types.PhotoStrippedSize):
                    return base64.b64encode(stripped_photo_to_jpg(size.bytes)).decode()
        if doc:
            for size in doc.thumbs:
                if isinstance(size, types.PhotoStrippedSize):
                    return base64.b64encode(stripped_photo_to_jpg(size.bytes)).decode()
    except Exception:
        pass
    return None


def _fname(msg: Any) -> str:
    doc = getattr(msg, "document", None)
    if doc:
        for a in doc.attributes:
            if hasattr(a, "file_name") and a.file_name:
                return a.file_name
    return f"{msg.id}{_ext(msg)}"


def _safe_name(s: str) -> str:
    return re.sub(r"[^\w\s-]", "", s).strip().replace(" ", "_") or "channel"


def _dimensions(msg: Any) -> tuple[int, int]:
    doc = getattr(msg, "document", None)
    if doc:
        for a in doc.attributes:
            if hasattr(a, "w") and hasattr(a, "h"):
                return a.w, a.h
    photo = getattr(msg, "photo", None)
    if photo:
        sizes = [s for s in getattr(photo, "sizes", []) if hasattr(s, "w") and hasattr(s, "h")]
        if sizes:
            best = sizes[-1]
            return getattr(best, "w", 0), getattr(best, "h", 0)
    return 0, 0


def _msg_to_item(msg: Any, channel_id: int) -> Optional[dict]:
    mt = _media_type(msg)
    if not mt:
        return None
    w, h = _dimensions(msg)
    return {
        "msg_id": msg.id,
        "channel_id": channel_id,
        "type": mt,
        "filename": _fname(msg),
        "size": _size(msg),
        "size_readable": _hr_size(_size(msg)),
        "date": msg.date.isoformat() if msg.date else None,
        "date_ts": int(msg.date.timestamp()) if msg.date else 0,
        "caption": (msg.message or "").strip(),
        "st_b64": _get_stripped_thumb(msg),
        "w": w,
        "h": h,
        "has_media": bool(getattr(msg, 'media', None))
    }


_thumb_sem = asyncio.Semaphore(3)

async def _fetch_thumb(channel_id: int, msg_id: int) -> Optional[bytes]:
    """
    Fetch thumbnail for a message. Checks RAM/Disk first. 
    If missing, fetches from Telegram (stripped then full download).
    """
    key = f"{channel_id}_{msg_id}"
    t_path = THUMBS_DIR / f"{key}.webp"
    
    # 1. RAM (O(1))
    data = st.thumbs.get(key)
    if data: return data
    
    # 2. Disk (Persistent)
    for ext in (".webp", ".jpg"):
        p = THUMBS_DIR / f"{key}{ext}"
        if p.exists():
            try:
                data = p.read_bytes()
                st.thumbs.set(key, data)
                return data
            except: pass
        
    # 3. Telegram — try stripped thumb first (no download, instant)
    async with _thumb_sem:
        try:
            c = await _client()
            entity = await _get_entity_robust(c, channel_id)
            msg = await c.get_messages(entity, ids=msg_id)

            thumb_bytes = None
            media = msg.media if msg else None
            if media:
                photo = getattr(media, 'photo', None) or getattr(media, 'document', None)
                if photo:
                    for sz in getattr(photo, 'thumbs', []) or []:
                        # PhotoStrippedSize — reconstruct JPEG using Telethon utility
                        if isinstance(sz, types.PhotoStrippedSize):
                            from telethon.utils import stripped_photo_to_jpg
                            thumb_bytes = stripped_photo_to_jpg(sz.bytes)
                        elif hasattr(sz, 'bytes') and sz.bytes:
                            thumb_bytes = sz.bytes
                            break

            if not thumb_bytes:
                # Fallback: download smallest thumb
                thumb_bytes = await c.download_media(msg, bytes, thumb=-1)

            if thumb_bytes:
                # Optimize to WebP
                from PIL import Image
                import io
                try:
                    with Image.open(io.BytesIO(thumb_bytes)) as img:
                        img.thumbnail((320, 320)) # Ensure standard size
                        out = io.BytesIO()
                        img.save(out, format="WEBP", quality=80)
                        thumb_webp = out.getvalue()
                        st.thumbs.set(key, thumb_webp)
                        t_path.write_bytes(thumb_webp)
                        return thumb_webp
                except Exception as e:
                    magic = thumb_bytes[:16].hex() if thumb_bytes else "None"
                    logger.warning(f"WebP conversion failed (Magic: {magic}), saving raw: {e}")
                    st.thumbs.set(key, thumb_bytes)
                    t_path.write_bytes(thumb_bytes)
                    return thumb_bytes
        except Exception as e:
            logger.error(f"Failed to fetch thumbnail for {channel_id}_{msg_id}: {e}")
    
    return None
    
    return None


async def _thumb_worker():
    """Background worker that silently pre-fetches thumbnails from the queue."""
    logger.info("Thumbnail pre-fetch worker started")
    
    async def worker():
        while True:
            cid, mid = await st.thumb_queue.get()
            try:
                # Direct check first before calling expensive _fetch_thumb
                has_thumb = any((THUMBS_DIR / f"{cid}_{mid}{ext}").exists() for ext in (".webp", ".jpg"))
                if not has_thumb:
                    await _fetch_thumb(cid, mid)
            except errors.FloodWaitError as e:
                logger.warning(f"Thumb pre-fetch rate limited, pausing {e.seconds}s")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logger.debug(f"Background thumb fetch failed for {cid}_{mid}: {e}")
            finally:
                st.thumb_queue.task_done()

    # Create 4 concurrent worker consumers
    for _ in range(4):
        asyncio.create_task(worker())


async def _media_sse(
    channel_ids: list[int],
    mtype: str,
    request: Request,
) -> AsyncGenerator[str, None]:
    c = await _client()
    fmap = {
        "photo": InputMessagesFilterPhotos(),
        "video": InputMessagesFilterVideo(),
        "document": InputMessagesFilterDocument(),
    }
    tf = fmap.get(mtype)

    for cid in channel_ids:
        try:
            # Send status update for entity resolution
            yield f"data: {json.dumps({'status': f'Opening channel {cid}...'})}\n\n"
            # Marked IDs (negative) allow get_entity to perfectly resolve from session cache
            entity = await _get_entity_robust(c, cid)
        except Exception as e:
            logger.error(f"Could not resolve entity for ID {cid}: {e}")
            yield f"data: {json.dumps({'error': f'Could not open channel {cid}: {str(e)}'})}\n\n"
            continue

        # Check cache first for this channel
        cached = await _db_read(lambda: _db_get_media(cid, mtype))
        last_cached = await _db_read(lambda: _db_last_msg_id(cid))
        if cached:
            # Emit ALL cached items as a single batch event — far faster than N events
            batch = [dict(r, size_readable=_hr_size(r["size"])) for r in cached]
            yield f"data: {json.dumps({'batch': batch})}\n\n"
            
            for r in cached:
                has_thumb = any((THUMBS_DIR / f"{cid}_{r['msg_id']}{ext}").exists() for ext in (".webp", ".jpg"))
                if not has_thumb:
                    st.thumb_queue.put_nowait((cid, r["msg_id"]))
        
        # Then fetch only NEW messages since last cached
        min_id = last_cached

        buf = []
        new_batch = []
        async for msg in _iter_with_retry(c, entity, filter=tf, limit=None,
                                          min_id=min_id):
            if await request.is_disconnected():
                return
            try:
                item = _msg_to_item(msg, cid)
            except Exception as e:
                logger.error(f"Error processing message {msg.id} in {cid}: {e}", exc_info=True)
                continue
                
            if not item:
                continue
            
            buf.append(item)
            new_batch.append(item)
            
            # Yield new items in batches of 50 for smoother UI
            if len(new_batch) >= 50:
                yield f"data: {json.dumps({'batch': new_batch})}\n\n"
                new_batch = []

            # Send a progress ping every 200 items scanned even if not yielded yet
            if (len(buf) + len(new_batch)) % 200 == 0:
                # Use a dummy key or just a status string
                yield f"data: {json.dumps({'status': f'Scanning... (found {len(buf) + len(new_batch)} items)'})}\n\n"

            if len(buf) >= 100:
                await _db_run(lambda b=list(buf): _db_cache_media_batch(b))
                # Trigger thumb pre-fetch for the batch
                for i in buf:
                    if i.get("has_media"):
                        has_thumb = any((THUMBS_DIR / f"{cid}_{i['msg_id']}{ext}").exists() for ext in (".webp", ".jpg"))
                        if not has_thumb:
                            st.thumb_queue.put_nowait((cid, i['msg_id']))
                buf.clear()

        if buf:
            await _db_run(lambda b=list(buf): _db_cache_media_batch(b))
            for i in buf:
                if i.get("has_media"):
                    has_thumb = any((THUMBS_DIR / f"{cid}_{i['msg_id']}{ext}").exists() for ext in (".webp", ".jpg"))
                    if not has_thumb:
                        st.thumb_queue.put_nowait((cid, i["msg_id"]))
            buf.clear()

    yield 'data: {"done":true}\n\n'


async def _cleanup_job(job_id: str) -> None:
    await asyncio.sleep(JOB_TTL_SECONDS) # Keep for 5 mins
    st.jobs.pop(job_id, None)
    st.cancel_evts.pop(job_id, None)


async def _run_download(job_id: str, items: list[dict]) -> None:
    c = await _client()
    job = st.jobs[job_id]
    cancel = st.cancel_evts[job_id]
    job.update({"total": len(items), "done": 0, "skipped": 0,
                "status": "running", "files": [], "errors": []})

    def log(msg):
        job["logs"] = (job.get("logs", []) + [f"{datetime.now().strftime('%H:%M:%S')} {msg}"])[-100:]

    async def one(item: dict) -> None:
        if cancel.is_set(): return
        msg_id, cid = item["msg_id"], item["channel_id"]
        fname = item["filename"] or f"file_{msg_id}"
        log(f"Processing: {fname}")
        
        # Check cache
        existing = await _db_read(lambda: _db_is_downloaded(cid, msg_id))
        if existing and Path(existing).exists():
            log(f"Skipped (cached): {fname}")
            job["skipped"] = job.get("skipped", 0) + 1
            job["done"] = job.get("done", 0) + 1
            return

        try:
            for attempt in range(3):
                try:
                    entity = await c.get_entity(cid)
                    folder = DOWNLOADS / _safe_name(getattr(entity, "title", str(cid)))
                    folder.mkdir(parents=True, exist_ok=True)
                    dest = folder / fname

                    if dest.exists():
                        log(f"Linked existing: {fname}")
                        await _db_run(lambda: _db_mark_downloaded(cid, msg_id, str(dest)))
                        job["skipped"] = job.get("skipped", 0) + 1
                        break

                    msg = await c.get_messages(entity, ids=msg_id)
                    def _prog(recv, total_bytes):
                        job["current"] = fname
                        job["pct"] = round(recv / total_bytes * 100, 1) if total_bytes else 0

                    log(f"Downloading media: {fname}")
                    await c.download_media(msg, file=str(dest), progress_callback=_prog)
                    # Save caption
                    cap = (msg.message or "").strip()
                    if cap: dest.with_suffix(".txt").write_text(cap, encoding="utf-8")
                    
                    await _db_run(lambda: _db_mark_downloaded(cid, msg_id, str(dest)))
                    job["files"].append(str(dest))
                    log(f"Saved: {fname}")
                    break

                except errors.FloodWaitError as e:
                    log(f"Rate limited: wait {e.seconds}s")
                    job["flood_wait"] = e.seconds
                    await asyncio.sleep(e.seconds)
                    job.pop("flood_wait", None)
                except Exception as e:
                    log(f"Error {fname}: {e}")
                    job["errors"].append(f"{fname}: {e}")
                    break
        finally:
            job["done"] = job.get("done", 0) + 1
            job["pct"] = round(job["done"] / job["total"] * 100, 1)

    job.update({"status": "running", "logs": [], "files": [], "errors": []})
    log(f"Started batch download of {job['total']} items")
    
    sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    async def bounded(item: dict) -> None:
        async with sem: await one(item)

    try:
        await asyncio.gather(*[bounded(i) for i in items])
        job.update({"status": "done", "pct": 100})
    except asyncio.CancelledError:
        job["status"] = "cancelled"
    except Exception as e:
        job.update({"status": "error", "current": str(e)})
    finally:
        st.cancel_evts.pop(job_id, None)
        st.tasks.pop(job_id, None)
    log(f"Batch {job['status']}")
    asyncio.create_task(_cleanup_job(job_id))


async def _run_ytdlp(job_id: str, url: str, fmt: str) -> None:
    import shutil
    job = st.jobs[job_id]
    cancel = st.cancel_evts[job_id]
    out_dir = DOWNLOADS / "external"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Prefer yt-dlp binary, fallback to python -m
    ytdlp_bin = shutil.which("yt-dlp")
    if ytdlp_bin:
        cmd = [ytdlp_bin]
    else:
        cmd = [sys.executable, "-m", "yt_dlp"]

    cmd += ["--newline", "--no-playlist",
            "-o", str(out_dir / "%(title)s.%(ext)s")]

    if fmt == "audio":
        # Best audio → extract to mp3
        cmd += ["-f", "bestaudio/best",
                "--extract-audio", "--audio-format", "mp3"]
    else:
        # Max quality video+audio → remux to mp4
        cmd += ["-f", "bv*+ba/b",
                "--merge-output-format", "mp4",
                "--remux-video", "mp4"]

    cmd.append(url)

    job.update({"status": "running", "url": url, "current": "Starting…", "pct": 0})
    prog_re = re.compile(r"\[download\]\s+([\d.]+)%")
    last_line = ""

    try:
        # Ensure ffmpeg is on PATH for merge/remux/extract
        env = os.environ.copy()
        env["PATH"] = "/opt/homebrew/bin:" + env.get("PATH", "")
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )
        async for raw in proc.stdout:
            if cancel.is_set():
                proc.terminate()
                break
            line = raw.decode(errors="ignore").strip()
            if line:
                last_line = line
            m = prog_re.search(line)
            if m:
                job["pct"] = float(m.group(1))
                job["current"] = line
            elif line.startswith("ERROR"):
                job["current"] = line
        rc = await proc.wait()
        if cancel.is_set():
            job.update({"status": "cancelled", "pct": 0, "current": None})
        elif rc != 0:
            job.update({"status": "error", "current": last_line or f"yt-dlp exited with code {rc}"})
        else:
            job.update({"status": "done", "pct": 100, "current": None})
    except asyncio.CancelledError:
        job["status"] = "cancelled"
    except Exception as e:
        job.update({"status": "error", "current": str(e)})
    finally:
        st.cancel_evts.pop(job_id, None)
        st.tasks.pop(job_id, None)
        asyncio.create_task(_cleanup_job(job_id))


async def _run_mirror(job_id: str, src_id: int, dst_id: int, limit: Optional[int]) -> None:
    c = await _client()
    job = st.jobs[job_id]
    cancel = st.cancel_evts[job_id]
    job.update({
        "status": "running", "total": job.get("total", 0), "done": job.get("done", 0), "pct": job.get("pct", 0), 
        "source_id": src_id, "target_id": dst_id, "logs": job.get("logs", [])
    })
    
    log_batch_cnt = 0
    def log(msg, force=False):
        nonlocal log_batch_cnt
        logger.info(f"[{job_id}] {msg}")
        job["logs"] = (job.get("logs", []) + [f"{datetime.now().strftime('%H:%M:%S')} {msg}"])[-500:]
        
        log_batch_cnt += 1
        if force or log_batch_cnt >= 5: # Reduced batch size for more real-time dashboard updates
            log_batch_cnt = 0
            # Auto-upsert status to DB
            asyncio.create_task(_db_run(lambda: _db_upsert_mirror({
                "id": job_id, "source_id": src_id, "target_id": dst_id,
                "status": job["status"], "total": job["total"], "current": job.get("done", 0),
                "last_msg_id": job.get("last_msg_id", 0),
                "logs": job.get("logs", [])
            })))

    try:
        log(f"Starting mirror from {src_id} to {dst_id}", force=True)
        src = await _get_entity_robust(c, src_id)
        dst = await _get_entity_robust(c, dst_id)
        
        # 1. Collect messages
        log("Scanning source channel for new messages...", force=True)
        msgs = []
        async for m in c.iter_messages(src, limit=limit, reverse=True):
            if cancel.is_set(): break
            if isinstance(m.action, types.MessageActionEmpty) or not m.action:
                msgs.append(m)
            if len(msgs) % 500 == 0: log(f"Found {len(msgs)} messages so far...")
        
        if not msgs:
            log("No messages found.")
            job.update({"status": "done", "pct": 100})
            return

        job["total"] = len(msgs)
        log(f"Found {len(msgs)} messages. Scanning destination for duplicates...", force=True)
        
        # 2. Pre-flight dedup: scan destination for already-present content
        dst_fingerprints = set()
        try:
            scan_limit = min(len(msgs) * 2, 15000)
            async for dm in c.iter_messages(dst, limit=scan_limit):
                if cancel.is_set(): break
                fp = _msg_fingerprint(dm)
                if fp:
                    dst_fingerprints.add(fp)
        except Exception as e:
            log(f"⚠ Destination scan partial: {e}")
        
        if dst_fingerprints:
            log(f"Found {len(dst_fingerprints)} existing items in destination (dedup active)", force=True)
        else:
            log("Destination is clean. Starting clone...", force=True)
        
        # 2. Clone loop
        i = 0
        while i < len(msgs):
            m = msgs[i]
            if cancel.is_set(): break
            
            # Deduplication Check (DB mapping + content fingerprint)
            if await _db_run(lambda: _db_is_mirrored(src_id, m.id, dst_id)):
                log(f"Skipping duplicate msg {m.id} (DB)")
                i += 1
                job["done"] = i
                job["pct"] = round(i / job["total"] * 100, 1)
                continue
            
            fp = _msg_fingerprint(m)
            if fp and fp in dst_fingerprints:
                log(f"Skipping msg {m.id} (already in destination)")
                # Back-fill DB mapping so future runs skip instantly
                await _db_run(lambda: _db_add_mirror_mapping(src_id, m.id, dst_id))
                i += 1
                job["done"] = i
                job["pct"] = round(i / job["total"] * 100, 1)
                continue

            try:
                # Send with automatic file reference refresh on expiry
                try:
                    await c.send_message(dst, m, comment_to=None)
                except Exception as _fre:
                    if not _is_file_ref_error(_fre): raise
                    log(f"⟳ File ref expired for msg {m.id}, refreshing...")
                    m = await c.get_messages(src, ids=m.id)
                    if not m:
                        log(f"⚠ Msg {msgs[i].id} no longer exists, skipping")
                        i += 1; job["done"] = i; job["pct"] = round(i / job["total"] * 100, 1)
                        continue
                    msgs[i] = m
                    await c.send_message(dst, m, comment_to=None)
                sz = _hr_size(_size(m)) if m.media else "text"
                log(f"Cloned msg {m.id} ({sz})")
                await _db_run(lambda: _db_add_mirror_mapping(src_id, m.id, dst_id))
                # Track fingerprint to prevent intra-batch duplicates
                if fp: dst_fingerprints.add(fp)
            except errors.ChatForwardsRestrictedError:
                sz = _hr_size(_size(m)) if m.media else "text"
                log(f"Msg {m.id} is protected ({sz}). Re-uploading...")
                if m.media:
                    from io import BytesIO
                    import mimetypes
                    bio = BytesIO()
                    log(f" ↓ Downloading msg {m.id} to buffer...")
                    try:
                        await c.download_media(m, bio)
                    except Exception as _fre:
                        if not _is_file_ref_error(_fre): raise
                        log(f"⟳ File ref expired during download, refreshing...")
                        m = await c.get_messages(src, ids=m.id)
                        if not m:
                            log(f"⚠ Msg deleted from source, skipping")
                            i += 1; job["done"] = i; job["pct"] = round(i / job["total"] * 100, 1)
                            continue
                        msgs[i] = m
                        bio = BytesIO()
                        await c.download_media(m, bio)
                    bio.seek(0)
                    
                    # 1. Try m.file.name (strongest)
                    fn = m.file.name
                    
                    # 2. Try attributes fallback
                    doc = getattr(m.media, 'document', None)
                    attrs = doc.attributes if doc else []
                    if not fn and doc:
                        for a in attrs:
                            if hasattr(a, 'file_name'): fn = a.file_name; break
                    
                    # 3. Guess extension from mime type
                    if not fn and doc and doc.mime_type:
                        ext = mimetypes.guess_extension(doc.mime_type)
                        if ext: fn = f"file_{m.id}{ext}"
                    
                    # 4. Final generic fallbacks
                    if not fn:
                        if isinstance(m.media, types.MessageMediaPhoto): fn = f"image_{m.id}.jpg"
                        else: fn = f"file_{m.id}.dat"

                    bio.name = fn
                    log(f" ↑ Uploading {fn} to target...")
                    await c.send_file(dst, bio, caption=m.message, formatting_entities=m.entities, attributes=attrs)
                    log(f"✅ Re-uploaded: {fn} ({sz})")
                else:
                    await c.send_message(dst, m.message, formatting_entities=m.entities)
                    log(f"✅ Synced text: {m.id}")
                
                # Save mapping to prevent future duplicates
                await _db_run(lambda: _db_add_mirror_mapping(src_id, m.id, dst_id))
                if fp: dst_fingerprints.add(fp)
            except errors.FloodWaitError as e:
                log(f"⚠ Rate limited: waiting {e.seconds}s...")
                await asyncio.sleep(e.seconds)
                continue
            except Exception as e:
                log(f"❌ Failed msg {m.id}: {e}")
            
            i += 1
            job["done"] = i
            job["pct"] = round(i / job["total"] * 100, 1)
            job["last_msg_id"] = m.id
            if (i+1) % 5 == 0: log(f"Progress: {i+1}/{len(msgs)}")
            await asyncio.sleep(0.1) # Faster with high RAM and optimized concurrency

        job["status"] = "done" if not cancel.is_set() else "cancelled"
        log(f"Job {job['status']}", force=True)

    except asyncio.CancelledError:
        job["status"] = "cancelled"
        log("Mirror job cancelled by user.", force=True)
    except Exception as e:
        logger.error(f"Mirror job {job_id} failed: {e}", exc_info=True)
        job.update({"status": "error", "current": str(e)})
        log(f"❌ Error: {e}", force=True)
    finally:
        st.cancel_evts.pop(job_id, None)
        st.tasks.pop(job_id, None)
        asyncio.create_task(_cleanup_job(job_id))


async def _restore_jobs():
    """Load previous mirror jobs from DB on startup, including sync_activity logs."""
    rows = await _db_run(_db_get_mirrors)
    # 1. Collect and deduplicate jobs
    active_pairs = set()
    resumable_tasks = [] # (jid, src_id, dst_id)
    
    # We iterate reversed (newest first) to prioritize fresh jobs
    for r in sorted(rows, key=lambda x: x["id"], reverse=True):
        jid = r["id"]
        
        def _safe_int(v):
            try: return int(v) if v is not None else 0
            except: return 0

        total = _safe_int(r["total"])
        current_num = _safe_int(r["current"])
        
        job = {
            "id": jid, "source_id": r["source_id"], "target_id": r["target_id"],
            "status": r["status"],
            "type": "mirror",
            "total": total,
            "done": current_num if r["status"] == "done" else 0,
            "current": r["current"] if isinstance(r["current"], str) and not r["current"].isdigit() else None,
            "logs": json.loads(r["logs"] or '[]'),
            "pct": round(current_num / total * 100, 1) if total > 0 else 0
        }
        
        if jid == "sync_activity":
            st.jobs["sync_activity"] = job
            continue

        # Singleton check: don't auto-resume multiple jobs for the same pair
        pair = (r["source_id"], r["target_id"])
        if job["status"] in ("running", "queued", "cancelled"):
            if pair in active_pairs:
                # Mark as cancelled if it's a duplicate of a newer job
                job["status"] = "cancelled"
                job["logs"].append(f"{datetime.now().strftime('%H:%M:%S')} Superseded by newer job.")
            elif pair[0] and pair[1]:
                active_pairs.add(pair)
                resumable_tasks.append((jid, pair[0], pair[1], job))
                job["status"] = "queued"
                job["logs"] = (job["logs"] + [f"{datetime.now().strftime('%H:%M:%S')} Resuming background mirror..."])
        
        st.jobs[jid] = job

    # 2. Start only the unique resumable tasks
    for jid, src_id, dst_id, job in resumable_tasks:
        st.cancel_evts[jid] = asyncio.Event()
        st.tasks[jid] = asyncio.create_task(_run_mirror(jid, src_id, dst_id, None))
        logger.info(f"Auto-Resumed singleton mirror: {jid} ({src_id} -> {dst_id})")
