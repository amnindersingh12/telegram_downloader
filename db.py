import asyncio, sqlite3
from datetime import datetime
from typing import Optional, Callable, Any
from config import DB_FILE

_db_write_lock = asyncio.Lock()
_conn_cache: sqlite3.Connection | None = None


def _db_connect() -> sqlite3.Connection:
    global _conn_cache
    if _conn_cache is None:
        _conn_cache = sqlite3.connect(DB_FILE, check_same_thread=False)
        _conn_cache.row_factory = sqlite3.Row
    return _conn_cache


def _db_init() -> None:
    with _db_connect() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA cache_size=-8000")       # 8MB in-memory cache
        c.execute("PRAGMA temp_store=MEMORY")
        c.execute("PRAGMA mmap_size=67108864")      # 64MB memory-mapped I/O
        c.executescript("""
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY, title TEXT, type TEXT,
            members INTEGER, username TEXT, cached_at REAL
        );
        CREATE TABLE IF NOT EXISTS media (
            channel_id INTEGER, msg_id INTEGER, type TEXT,
            filename TEXT, size INTEGER, date TEXT, caption TEXT DEFAULT '',
            PRIMARY KEY (channel_id, msg_id)
        );
        CREATE TABLE IF NOT EXISTS downloads (
            channel_id INTEGER, msg_id INTEGER, filepath TEXT,
            downloaded_at REAL, PRIMARY KEY (channel_id, msg_id)
        );
        CREATE INDEX IF NOT EXISTS idx_media_channel ON media(channel_id, msg_id DESC);
        CREATE INDEX IF NOT EXISTS idx_media_type ON media(channel_id, type);
        CREATE INDEX IF NOT EXISTS idx_downloads_lookup ON downloads(channel_id, msg_id);
        """)
        # Migrate older DBs that lack the caption column
        cols = [r["name"] for r in c.execute("PRAGMA table_info(media)").fetchall()]
        if "caption" not in cols:
            c.execute("ALTER TABLE media ADD COLUMN caption TEXT DEFAULT ''")
        if "w" not in cols:
            c.execute("ALTER TABLE media ADD COLUMN w INTEGER DEFAULT 0")
        if "h" not in cols:
            c.execute("ALTER TABLE media ADD COLUMN h INTEGER DEFAULT 0")


async def _db_run(fn: Callable) -> Any:
    """Run a sync DB write inside the event loop, serialized."""
    loop = asyncio.get_running_loop()
    async with _db_write_lock:
        return await loop.run_in_executor(None, fn)


async def _db_read(fn: Callable) -> Any:
    """Run a sync DB read without locking — WAL supports concurrent readers."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, fn)


def _db_upsert_channel(ch: dict) -> None:
    with _db_connect() as c:
        c.execute(
            "INSERT OR REPLACE INTO channels VALUES (?,?,?,?,?,?)",
            (ch["id"], ch["title"], ch["type"], ch.get("members"),
             ch.get("username"), datetime.now().timestamp()),
        )


def _db_get_channels() -> list[dict]:
    with _db_connect() as c:
        rows = c.execute("SELECT * FROM channels ORDER BY title").fetchall()
        return [dict(r) for r in rows]


def _db_cache_media(item: dict) -> None:
    _db_cache_media_batch([item])


def _db_cache_media_batch(items: list[dict]) -> None:
    if not items:
        return
    with _db_connect() as c:
        c.executemany(
            "INSERT OR IGNORE INTO media VALUES (?,?,?,?,?,?,?)",
            [(i["channel_id"], i["msg_id"], i["type"],
              i["filename"], i["size"], i.get("date"), i.get("caption", ""))
             for i in items],
        )


def _db_get_media(channel_id: int, mtype: str = "all") -> list[dict]:
    with _db_connect() as c:
        if mtype == "all":
            rows = c.execute(
                "SELECT * FROM media WHERE channel_id=? ORDER BY msg_id DESC",
                (channel_id,)
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM media WHERE channel_id=? AND type=? ORDER BY msg_id DESC",
                (channel_id, mtype)
            ).fetchall()
        return [dict(r) for r in rows]


def _db_last_msg_id(channel_id: int) -> int:
    with _db_connect() as c:
        row = c.execute(
            "SELECT MAX(msg_id) FROM media WHERE channel_id=?", (channel_id,)
        ).fetchone()
        return row[0] or 0


def _db_mark_downloaded(channel_id: int, msg_id: int, filepath: str) -> None:
    with _db_connect() as c:
        c.execute(
            "INSERT OR REPLACE INTO downloads VALUES (?,?,?,?)",
            (channel_id, msg_id, filepath, datetime.now().timestamp()),
        )


def _db_is_downloaded(channel_id: int, msg_id: int) -> Optional[str]:
    with _db_connect() as c:
        row = c.execute(
            "SELECT filepath FROM downloads WHERE channel_id=? AND msg_id=?",
            (channel_id, msg_id)
        ).fetchone()
        return row["filepath"] if row else None
