import asyncio, json, logging, os, re, sys
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Optional, Any
from fastapi import HTTPException, Request
from telethon import TelegramClient, errors
from telethon.tl import types
from telethon.tl.types import (
    InputMessagesFilterPhotos, InputMessagesFilterVideo, InputMessagesFilterDocument
)
from config import SESSION, DOWNLOADS, THUMB_CACHE_SIZE, MAX_CONCURRENT_DOWNLOADS, JOB_TTL_SECONDS
from db import _db_run, _db_read, _db_get_media, _db_last_msg_id, _db_cache_media, \
    _db_is_downloaded, _db_mark_downloaded, _db_cache_media_batch, _db_upsert_mirror, \
    _db_get_mirrors, _db_is_mirrored, _db_add_mirror_mapping, _db_get_sync_rules, _db_add_sync_rule, _db_remove_sync_rule
from telethon import TelegramClient, errors, events

logger = logging.getLogger("tgrab")

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
    client:      Optional[TelegramClient] = None
    api_id:      Optional[int]  = None
    api_hash:    Optional[str]  = None
    phone:       Optional[str]  = None
    phone_hash:  Optional[str]  = None
    queues:      set[asyncio.Queue] = set() # SSE event queues
    jobs:        dict[str, dict] = {
        "sync_activity": {"status": "running", "type": "sync", "logs": ["Live Sync service started..."], "pct": 100}
    }
    cancel_evts: dict[str, asyncio.Event] = {}
    sync_map:    dict[int, set[int]] = {} # source_id -> {target_ids}
    thumbs:      LRU = LRU(THUMB_CACHE_SIZE)

st = _State()


@events.register(events.NewMessage)
async def _on_new_message(event):
    src_id = event.chat_id
    if src_id not in st.sync_map: return
    
    m = event.message
    targets = st.sync_map[src_id]
    
    for dst_id in targets:
        try:
            # Check for duplicate (though NewMessage shouldn't be duplicate, it's safe)
            if await _db_run(lambda: _db_is_mirrored(src_id, m.id, dst_id)): continue

            try:
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
            
            await _db_run(lambda: _db_add_mirror_mapping(src_id, m.id, dst_id))
            logger.info(f"Live Sync: Cloned {src_id} -> {dst_id}")
            # Update UI logs
            from datetime import datetime
            log_msg = f"{datetime.now().strftime('%H:%M:%S')} Sync: Cloned {src_id} -> {dst_id}"
            st.jobs["sync_activity"]["logs"] = (st.jobs["sync_activity"].get("logs", []) + [log_msg])[-100:]
        except Exception as e:
            logger.error(f"Live Sync Error: {e}")
            from datetime import datetime
            st.jobs["sync_activity"]["logs"] = (st.jobs["sync_activity"].get("logs", []) + [f"{datetime.now().strftime('%H:%M:%S')} Error: {e}"])[-100:]
        
        # Persist sync_activity state
        await _db_run(lambda: _db_upsert_mirror({
            "id": "sync_activity", "source_id": 0, "target_id": 0,
            "status": "running", "total": 0, "current": None,
            "logs": st.jobs["sync_activity"].get("logs", [])
        }))
    
    # Notify connected clients about new activity (Unread updates)
    for q in st.queues:
        q.put_nowait({
            "type": "new_message",
            "channel_id": src_id,
            "msg_id": m.id
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
    try:
        return await client.get_entity(peer)
    except Exception:
        # Only sweep if peer looks like an ID
        is_id = False
        pid = 0
        if isinstance(peer, (int, float)):
            is_id = True; pid = int(peer)
        elif isinstance(peer, str):
            clean = peer.lstrip('-')
            if clean.isdigit(): is_id = True; pid = int(peer)
        
        if is_id:
            logger.info(f"Entity {pid} not in cache — sweeping dialogs...")
            async for d in client.iter_dialogs(limit=None):
                if d.id == pid:
                    return d.entity
        raise


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
        "caption": (msg.message or "").strip(),
        "w": w,
        "h": h,
    }


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
            # Marked IDs (negative) allow get_entity to perfectly resolve from session cache
            entity = await _get_entity_robust(c, cid)
        except Exception as e:
            logger.error(f"Could not resolve entity for ID {cid}: {e}")
            yield f"data: {json.dumps({'error': str(e), 'channel_id': cid})}\n\n"
            continue

        # Check cache first for this channel
        cached = await _db_read(lambda: _db_get_media(cid, mtype))
        last_cached = await _db_read(lambda: _db_last_msg_id(cid))
        if cached:
            # Emit ALL cached items as a single batch event — far faster than N events
            batch = [dict(r, size_readable=_hr_size(r["size"])) for r in cached]
            yield f"data: {json.dumps({'batch': batch})}\n\n"
        
        # Then fetch only NEW messages since last cached
        min_id = last_cached

        buf = []
        async for msg in _iter_with_retry(c, entity, filter=tf, limit=None,
                                          min_id=min_id):
            if await request.is_disconnected():
                return
            item = _msg_to_item(msg, cid)
            if not item:
                continue
            
            buf.append(item)
            yield f"data: {json.dumps(item)}\n\n"
            
            if len(buf) >= 50:
                await _db_run(lambda b=list(buf): _db_cache_media_batch(b))
                buf.clear()
                await asyncio.sleep(0)
        
        if buf:
            await _db_run(lambda b=list(buf): _db_cache_media_batch(b))
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

    await asyncio.gather(*[bounded(i) for i in items])
    status = "cancelled" if cancel.is_set() else "done"
    job.update({"status": status, "pct": 100, "current": None})
    log(f"Batch {status}")
    st.cancel_evts.pop(job_id, None)
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
    except FileNotFoundError:
        job.update({"status": "error", "current": "yt-dlp not found — install with: pip install yt-dlp"})
    except Exception as e:
        job.update({"status": "error", "current": str(e)})
    finally:
        st.cancel_evts.pop(job_id, None)
        asyncio.create_task(_cleanup_job(job_id))


async def _run_mirror(job_id: str, src_id: int, dst_id: int, limit: Optional[int]) -> None:
    c = await _client()
    job = st.jobs[job_id]
    cancel = st.cancel_evts[job_id]
    job.update({
        "status": "running", "total": 0, "done": 0, "pct": 0, 
        "source_id": src_id, "target_id": dst_id, "logs": []
    })
    
    def log(msg):
        logger.info(f"[{job_id}] {msg}")
        job["logs"] = (job.get("logs", []) + [f"{datetime.now().strftime('%H:%M:%S')} {msg}"])[-100:]
        # Auto-upsert status to DB
        asyncio.create_task(_db_run(lambda: _db_upsert_mirror({
            "id": job_id, "source_id": src_id, "target_id": dst_id,
            "status": job["status"], "total": job["total"], "current": job.get("current"),
            "last_msg_id": job.get("last_msg_id", 0),
            "logs": job.get("logs", [])
        })))

    try:
        log(f"Starting mirror from {src_id} to {dst_id}")
        src = await _get_entity_robust(c, src_id)
        dst = await _get_entity_robust(c, dst_id)
        
        # 1. Collect messages
        log("Fetching messages...")
        msgs = []
        async for m in c.iter_messages(src, limit=limit, reverse=True):
            if cancel.is_set(): break
            if isinstance(m.action, types.MessageActionEmpty) or not m.action:
                msgs.append(m)
        
        if not msgs:
            log("No messages found.")
            job.update({"status": "done", "pct": 100})
            return

        job["total"] = len(msgs)
        log(f"Found {len(msgs)} messages. Starting clone...")
        
        # 2. Clone loop
        for i, m in enumerate(msgs):
            if cancel.is_set(): break
            
            # Deduplication Check
            if await _db_run(lambda: _db_is_mirrored(src_id, m.id, dst_id)):
                log(f"Skipping duplicate msg {m.id}")
                job["done"] = i + 1
                job["pct"] = round((i + 1) / job["total"] * 100, 1)
                continue

            try:
                # as_copy=True fallback via send_message
                await c.send_message(dst, m, comment_to=None)
                log(f"Cloned msg {m.id}")
            except errors.ChatForwardsRestrictedError:
                log(f"Msg {m.id} is protected. Re-uploading...")
                if m.media:
                    from io import BytesIO
                    import mimetypes
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
                    await c.send_file(dst, bio, caption=m.message, formatting_entities=m.entities, attributes=attrs)
                    log(f"Synced: {fn}")
                else:
                    await c.send_message(dst, m.message, formatting_entities=m.entities)
                    log(f"Synced text: {m.id}")
                
                # Save mapping to prevent future duplicates
                await _db_run(lambda: _db_add_mirror_mapping(src_id, m.id, dst_id))
            except Exception as e:
                log(f"Failed msg {m.id}: {e}")
            
            job["done"] = i + 1
            job["pct"] = round((i + 1) / job["total"] * 100, 1)
            job["last_msg_id"] = m.id
            if (i+1) % 5 == 0: log(f"Progress: {i+1}/{len(msgs)}")
            await asyncio.sleep(0.1) # Faster with high RAM and optimized concurrency

        job["status"] = "done" if not cancel.is_set() else "cancelled"
        log(f"Job {job['status']}")

    except Exception as e:
        logger.error(f"Mirror job {job_id} failed: {e}", exc_info=True)
        job.update({"status": "error", "current": str(e)})
    finally:
        st.cancel_evts.pop(job_id, None)
        asyncio.create_task(_cleanup_job(job_id))


async def _restore_jobs():
    """Load previous mirror jobs from DB on startup, including sync_activity logs."""
    rows = await _db_run(_db_get_mirrors)
    for r in rows:
        jid = r["id"]
        # Convert DB row to UI job format
        total = r["total"] or 0
        current = r["current"] or 0
        job = {
            "status": r["status"],
            "total": total,
            "done": current if r["status"] == "done" else 0, # Approximation for display
            "current": None,
            "logs": json.loads(r["logs"] or '[]'),
            "pct": round(current / total * 100, 1) if total > 0 else 0
        }
        
        if jid == "sync_activity":
            st.jobs["sync_activity"] = job
            continue

        # If it was running/queued, mark as interrupted/error on startup
        if job["status"] in ("running", "queued"):
            job["status"] = "error"
            job["logs"] = (job["logs"] + [f"{datetime.now().strftime('%H:%M:%S')} Job interrupted by server restart."])[-100:]
        
        st.jobs[jid] = job
