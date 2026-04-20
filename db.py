import asyncio, json, sqlite3
from datetime import datetime
from typing import Optional, Callable, Any
from config import DB_FILE

_db_write_lock = asyncio.Lock()
import threading

_local = threading.local()

def _db_connect() -> sqlite3.Connection:
    if not hasattr(_local, "conn"):
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # Enable WAL for new connections too
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _local.conn = conn
    return _local.conn


def _db_init() -> None:
    with _db_connect() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA cache_size=-128000")       # 128MB in-memory cache
        c.execute("PRAGMA temp_store=MEMORY")
        c.execute("PRAGMA mmap_size=67108864")      # 64MB memory-mapped I/O
        c.executescript("""
        CREATE TABLE IF NOT EXISTS entities (
            id INTEGER PRIMARY KEY, type TEXT, title TEXT,
            username TEXT, members INTEGER, can_post INTEGER,
            unread INTEGER DEFAULT 0, folders TEXT DEFAULT '[]'
        );
        CREATE TABLE IF NOT EXISTS media (
            channel_id INTEGER, msg_id INTEGER, type TEXT,
            filename TEXT, size INTEGER, date TEXT, caption TEXT DEFAULT '',
            w INTEGER DEFAULT 0, h INTEGER DEFAULT 0, tags TEXT DEFAULT '',
            PRIMARY KEY (channel_id, msg_id)
        );
        CREATE TABLE IF NOT EXISTS downloads (
            channel_id INTEGER, msg_id INTEGER, filepath TEXT,
            downloaded_at REAL, PRIMARY KEY (channel_id, msg_id)
        );
        CREATE TABLE IF NOT EXISTS mirrors (
            id TEXT PRIMARY KEY, source_id INTEGER, target_id INTEGER,
            last_msg_id INTEGER DEFAULT 0, status TEXT, 
            total INTEGER DEFAULT 0, current INTEGER DEFAULT 0,
            logs TEXT DEFAULT '[]'
        );
        CREATE TABLE IF NOT EXISTS mirrored_messages (
            source_id INTEGER, source_msg_id INTEGER, target_id INTEGER,
            PRIMARY KEY (source_id, source_msg_id, target_id)
        );
        CREATE TABLE IF NOT EXISTS sync_rules (
            source_id INTEGER, target_id INTEGER,
            PRIMARY KEY (source_id, target_id)
        );
        CREATE INDEX IF NOT EXISTS idx_media_channel ON media(channel_id, msg_id DESC);
        CREATE INDEX IF NOT EXISTS idx_media_date ON media(channel_id, date DESC);
        CREATE INDEX IF NOT EXISTS idx_media_filename ON media(filename COLLATE NOCASE);
        CREATE INDEX IF NOT EXISTS idx_media_type ON media(channel_id, type);
        CREATE INDEX IF NOT EXISTS idx_downloads_lookup ON downloads(channel_id, msg_id);
        CREATE INDEX IF NOT EXISTS idx_mirrored_lookup ON mirrored_messages(source_id, source_msg_id);
        """)
        # Migrate older DBs that lack the caption column
        cols = [r["name"] for r in c.execute("PRAGMA table_info(media)").fetchall()]
        if "caption" not in cols:
            c.execute("ALTER TABLE media ADD COLUMN caption TEXT DEFAULT ''")
        if "w" not in cols:
            c.execute("ALTER TABLE media ADD COLUMN w INTEGER DEFAULT 0")
        if "h" not in cols:
            c.execute("ALTER TABLE media ADD COLUMN h INTEGER DEFAULT 0")
        
        m_cols = [r["name"] for r in c.execute("PRAGMA table_info(mirrors)").fetchall()]
        if "logs" not in m_cols:
            c.execute("ALTER TABLE mirrors ADD COLUMN logs TEXT DEFAULT '[]'")
        
        # Migration: Ensure channel IDs are marked (negative) as per Telethon standard.
        # This fixes issues with private channels where positive IDs are ambiguous.
        rows = c.execute("SELECT id, type FROM channels WHERE id > 0").fetchall()
        for r in rows:
            old_id = r["id"]
            if r["type"] == "channel":
                new_id = int("-100" + str(old_id))
            else: # group
                new_id = -old_id
            
            c.execute("UPDATE channels SET id=? WHERE id=?", (new_id, old_id))
            c.execute("UPDATE media SET channel_id=? WHERE channel_id=?", (new_id, old_id))
            c.execute("UPDATE downloads SET channel_id=? WHERE channel_id=?", (new_id, old_id))


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
            "INSERT OR IGNORE INTO media (channel_id, msg_id, type, filename, size, date, caption, w, h) VALUES (?,?,?,?,?,?,?,?,?)",
            [(i["channel_id"], i["msg_id"], i["type"],
              i["filename"], i["size"], i.get("date"), i.get("caption", ""),
              i.get("w", 0), i.get("h", 0))
             for i in items],
        )


def _db_get_media(channel_id: int, mtype: str = "all", limit: int = 5000, offset: int = 0) -> list[dict]:
    with _db_connect() as c:
        if mtype == "all":
            rows = c.execute(
                f"SELECT * FROM media WHERE channel_id=? ORDER BY msg_id DESC LIMIT {limit} OFFSET {offset}",
                (channel_id,)
            ).fetchall()
        else:
            rows = c.execute(
                f"SELECT * FROM media WHERE channel_id=? AND type=? ORDER BY msg_id DESC LIMIT {limit} OFFSET {offset}",
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

def _db_upsert_mirror(m: dict) -> None:
    with _db_connect() as c:
        c.execute("""
            INSERT INTO mirrors (id, source_id, target_id, last_msg_id, status, total, current, logs)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET 
                status=excluded.status, total=excluded.total, current=excluded.current, 
                last_msg_id=excluded.last_msg_id, logs=excluded.logs
        """, (m["id"], m["source_id"], m["target_id"], m.get("last_msg_id", 0), 
              m["status"], m["total"], m["current"], json.dumps(m.get("logs", []))))

def _db_get_mirrors() -> list[dict]:
    with _db_connect() as c:
        rows = c.execute("SELECT * FROM mirrors ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]

def _db_is_mirrored(src_id: int, src_msg_id: int, dst_id: int) -> bool:
    with _db_connect() as c:
        row = c.execute("SELECT 1 FROM mirrored_messages WHERE source_id=? AND source_msg_id=? AND target_id=?", (src_id, src_msg_id, dst_id)).fetchone()
        return bool(row)

def _db_add_mirror_mapping(src_id: int, src_msg_id: int, dst_id: int) -> None:
    with _db_connect() as c:
        c.execute("INSERT OR IGNORE INTO mirrored_messages (source_id, source_msg_id, target_id) VALUES (?, ?, ?)", (src_id, src_msg_id, dst_id))

def _db_get_sync_rules() -> list[dict]:
    with _db_connect() as c:
        rows = c.execute("SELECT * FROM sync_rules").fetchall()
        return [dict(r) for r in rows]

def _db_add_sync_rule(src_id: int, dst_id: int) -> None:
    with _db_connect() as c:
        c.execute("INSERT OR IGNORE INTO sync_rules (source_id, target_id) VALUES (?, ?)", (src_id, dst_id))

def _db_remove_sync_rule(src_id: int, dst_id: int) -> None:
    with _db_connect() as c:
        c.execute("DELETE FROM sync_rules WHERE source_id=? AND target_id=?", (src_id, dst_id))
