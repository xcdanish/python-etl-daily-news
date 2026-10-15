import hashlib
from datetime import datetime
import pandas as pd
import requests
from dagster import AssetExecutionContext, MetadataValue, asset, MaterializeResult
from dagster_duckdb import DuckDBResource
from ..resources import SqliteResource, PostgresResource

# 1. API INGESTION ASSET


@asset(compute_kind="HackerNews API")
def topstories_api_ingest(context: AssetExecutionContext, duckdb: DuckDBResource, postgres: PostgresResource) -> MaterializeResult:
    """Fetches the top 100 stories from the Hacker News API and persists them to DuckDB and PostgreSQL."""
    url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_ids = requests.get(url).json()[:100]

    results = []
    for item_id in top_ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        if item:
            results.append(item)

    df = pd.DataFrame(results)
    df["data_transfer_timestamp"] = pd.Timestamp.now()
    df["hashkey"] = df["title"].astype(str).apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest())

    with duckdb.get_connection() as conn:
        df.to_sql("api_ingest", con=conn, if_exists="append", index=False)
        total_num_records = conn.execute(
            "SELECT COUNT(*) FROM api_ingest").fetchone()[0]

    # Also persist records into PostgreSQL for external SQL client visibility (e.g., DBeaver)
    pg_records = 0
    try:
        conn = postgres.get_connection()
        cur = conn.cursor()
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO api_ingest (id, title, by, score, time, type, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    score = EXCLUDED.score;
            """, (
                int(row["id"]),
                str(row.get("title", "")),
                str(row.get("by", "")),
                int(row.get("score", 0)),
                int(row.get("time", 0)),
                str(row.get("type", "story")),
                str(row.get("url", ""))
            ))
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM api_ingest")
        pg_records = cur.fetchone()[0]
        cur.close()
        conn.close()
    except Exception as e:
        context.log.warning(f"PostgreSQL write warning: {e}")

    return MaterializeResult(
        metadata={
            "duckdb_records": total_num_records,
            "postgres_records": pg_records,
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

# 2. SQLITE INGESTION ASSET


@asset(compute_kind="sqlite")
def topstories_sqlite_ingest(context: AssetExecutionContext, sqlite: SqliteResource, duckdb: DuckDBResource) -> MaterializeResult:
    """Reads ingested stories from the SQLite database and loads them into the DuckDB warehouse."""
    conn = sqlite.get_connection()
    df = pd.read_sql("SELECT * FROM api_ingest", con=conn)
    conn.close()

    df["data_transfer_timestamp"] = pd.Timestamp.now()
    df["hashkey"] = df["title"].astype(str).apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest())

    with duckdb.get_connection() as conn:
        df.to_sql("sqlite_ingest", con=conn, if_exists="append", index=False)
        total_num_records = conn.execute(
            "SELECT COUNT(*) FROM sqlite_ingest").fetchone()[0]

    return MaterializeResult(
        metadata={
            "total_num_records": total_num_records,
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

# 3. TRANSFORMATION (MERGE & DEDUPLICATION)


@asset(deps=[topstories_api_ingest, topstories_sqlite_ingest], compute_kind="duckdb")
def common_table(duckdb: DuckDBResource) -> MaterializeResult:
    """Merges and deduplicates records from API and SQLite sources into a unified common_table in DuckDB."""
    with duckdb.get_connection() as conn:
        api_exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'api_ingest'"
        ).fetchone()[0] > 0
        sqlite_exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sqlite_ingest'"
        ).fetchone()[0] > 0

        # Create an empty counterpart table if only one source has run so far, preventing union query failures
        if not sqlite_exists and api_exists:
            conn.execute("CREATE TABLE sqlite_ingest AS SELECT * FROM api_ingest WHERE 1=0;")
        elif not api_exists and sqlite_exists:
            conn.execute("CREATE TABLE api_ingest AS SELECT * FROM sqlite_ingest WHERE 1=0;")

        COLUMNS = 'id, title, "by", score, "time", "type", url, hashkey'

        table_exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'common_table'"
        ).fetchone()[0] > 0

        if not table_exists:
            conn.execute(f"""
                CREATE TABLE common_table AS
                SELECT {COLUMNS} FROM api_ingest
                UNION ALL
                SELECT {COLUMNS} FROM sqlite_ingest
            """)
        else:
            conn.execute(f"""
                INSERT INTO common_table
                SELECT {COLUMNS} FROM api_ingest WHERE hashkey NOT IN (SELECT hashkey FROM common_table)
                UNION ALL
                SELECT {COLUMNS} FROM sqlite_ingest WHERE hashkey NOT IN (SELECT hashkey FROM common_table)
            """)

        total_num_records = conn.execute(
            "SELECT COUNT(*) FROM common_table").fetchone()[0]

    return MaterializeResult(metadata={"total_num_records": total_num_records})

# 4. TRANSFORMATION (ANALYTICS - TOP WORDS)


@asset(deps=[common_table], compute_kind="duckdb")
def most_frequent_title_words(duckdb: DuckDBResource) -> MaterializeResult:
    """Filters common stopwords and computes the top 25 most frequent keywords across story titles."""
    stopwords = {"a", "the", "an", "of", "to", "in",
                 "for", "and", "with", "on", "is", "by", "at"}

    with duckdb.get_connection() as conn:
        topstories = pd.read_sql("SELECT * FROM common_table", conn)

        word_counts = {}
        for raw_title in topstories["title"].dropna():
            for word in str(raw_title).lower().split():
                cleaned_word = word.strip(".,-!?:;()[]'\"-")
                if cleaned_word and cleaned_word not in stopwords:
                    word_counts[cleaned_word] = word_counts.get(
                        cleaned_word, 0) + 1

        top_words = sorted(word_counts.items(),
                           key=lambda x: x[1], reverse=True)[:25]
        df_top_words = pd.DataFrame(top_words, columns=["word", "count"])
        df_top_words.to_sql("top_words", con=conn,
                            if_exists="replace", index=False)

    return MaterializeResult(
        metadata={"top_words": MetadataValue.md(
            df_top_words.head().to_markdown())}
    )

# 5. FRONTEND NOTIFICATION TRIGGER


@asset(deps=[most_frequent_title_words], compute_kind="signal")
def streamlit_dashboard(context: AssetExecutionContext) -> MaterializeResult:
    """Emits an update signal timestamp to notify the Streamlit dashboard that fresh data is ready."""
    signal_file = "frontend/streamlit_reload_signal.txt"
    with open(signal_file, "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return MaterializeResult(
        metadata={"signal_update_time": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")}
    )
