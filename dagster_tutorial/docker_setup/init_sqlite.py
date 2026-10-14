import sqlite3
import pandas as pd

conn = sqlite3.connect("data/hackernews.sqlite")
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS api_ingest (
        id INTEGER PRIMARY KEY,
        title TEXT,
        by TEXT,
        score INTEGER,
        time INTEGER,
        type TEXT,
        url TEXT
    )
""")
# Dummy seed record
cursor.execute("""
    INSERT OR IGNORE INTO api_ingest (id, title, by, score, time, type, url)
    VALUES (1, 'Show HN: My New Dagster Docker Project', 'admin', 150, 1600000000, 'story', 'https://example.com')
""")
conn.commit()
conn.close()
print("SQLite database initialized successfully.")
