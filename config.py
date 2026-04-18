from pathlib import Path

BASE                     = Path(__file__).parent
SESSION                  = BASE / "tg"
CREDS_FILE               = BASE / "tg_creds.json"
DB_FILE                  = BASE / "cache.db"
_dl_pref                 = Path.home() / "Downloads" / "telegram"
DOWNLOADS                = _dl_pref if _dl_pref.parent.exists() else BASE / "downloads"
PORT                     = 7861
MAX_CONCURRENT_DOWNLOADS = 4
THUMB_CACHE_SIZE         = 500
JOB_TTL_SECONDS          = 300
THUMBS_DIR               = BASE / "cache" / "thumbs"
PREVIEWS                 = BASE / "previews"
