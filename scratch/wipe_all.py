import asyncio
import sqlite3
import sys
import os
from pathlib import Path

# Add current dir to path
sys.path.append(os.getcwd())

async def wipe():
    # 1. Database name from config/standard
    db_path = Path("cache.db")
    if not db_path.exists():
        print(f"Database {db_path} not found. Trying local search...")
        # Search for any .db files
        dbs = list(Path('.').glob('*.db'))
        if dbs: db_path = dbs[0]
        else:
            print("No database files found.")
            return

    # Wipe database logs
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        # Verify if 'mirrors' table exists
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mirrors'")
        if c.fetchone():
            c.execute("DELETE FROM mirrors")
            conn.commit()
            print(f"Database {db_path} mirror logs wiped successfully.")
        else:
            print(f"Table 'mirrors' not found in {db_path}.")
        conn.close()
    except Exception as e:
        print(f"Failed to wipe database: {e}")

if __name__ == "__main__":
    asyncio.run(wipe())
