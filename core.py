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
    _db_is_downloaded, _db_mark_downloaded, _db_cache_media_batch

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
    jobs:        dict[str, dict] = {}
    cancel_evts: dict[str, asyncio.Event] = {}
    thumbs:      LRU = LRU(THUMB_CACHE_SIZE)

st = _State()


def _mk_client(api_id: int, api_hash: str) -> TelegramClient:
    return TelegramClient(str(SESSION), api_id, api_hash)


async def _client() -> TelegramClient:
    if not st.client:
        raise HTTPException(503, "Not authenticated")
    if not st.client.is_connected():
        try:
            await st.client.connect()
            if not await st.client.is_user_authorized():
                raise HTTPException(503, "Session expired — please re-login")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(503, "Failed to reconnect to Telegram")
    return st.client


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
            entity = await c.get_entity(types.PeerChannel(cid))
        except Exception:
            try:
                entity = await c.get_entity(cid)
            except Exception as e:
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

    async def one(item: dict) -> None:
        if cancel.is_set():
            return
        cid, mid = item["channel_id"], item["msg_id"]
        fname = item.get("filename", f"{mid}.bin")

        # Resume: skip if already downloaded and file still exists
        existing = await _db_read(lambda: _db_is_downloaded(cid, mid))
        if existing and Path(existing).exists():
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

                    # Skip if file already on disk (even if not in DB)
                    if dest.exists():
                        await _db_run(lambda: _db_mark_downloaded(cid, mid, str(dest)))
                        job["skipped"] = job.get("skipped", 0) + 1
                        break # Success (skipped)

                    msg = await c.get_messages(entity, ids=mid)

                    def _prog(recv, total_bytes):
                        job["current"] = fname
                        job["pct"] = round(recv / total_bytes * 100, 1) if total_bytes else 0

                    await c.download_media(msg, file=str(dest), progress_callback=_prog)
                    # Save caption as sidecar .txt (only if non-empty)
                    caption = (msg.message or "").strip()
                    if caption:
                        dest.with_suffix(".txt").write_text(caption, encoding="utf-8")
                    await _db_run(lambda: _db_mark_downloaded(cid, mid, str(dest)))
                    job["files"].append(str(dest))
                    break # Success

                except errors.FloodWaitError as e:
                    job["flood_wait"] = e.seconds
                    job["current"] = f"⏳ Rate limited (attempt {attempt+1}/3) — waiting {e.seconds}s…"
                    await asyncio.sleep(e.seconds)
                    job.pop("flood_wait", None)
                    if attempt == 2:
                        job["errors"].append(f"{fname}: Max retries (FloodWait)")
                except Exception as e:
                    job["errors"].append(f"{fname}: {e}")
                    break # Non-recoverable or unknown error
        finally:
            job["done"] = job.get("done", 0) + 1
            job["current"] = fname
            job["pct"] = round(job["done"] / job["total"] * 100, 1)

    sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    async def bounded(item: dict) -> None:
        async with sem:
            await one(item)

    await asyncio.gather(*[bounded(i) for i in items])
    status = "cancelled" if cancel.is_set() else "done"
    job.update({"status": status, "pct": 100, "current": None})
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
