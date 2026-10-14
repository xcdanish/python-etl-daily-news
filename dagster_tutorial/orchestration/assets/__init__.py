import hashlib
from datetime import datetime
import pandas as pd
import requests
from dagster import AssetExecutionContext, MetadataValue, asset, MaterializeResult
from dagster_duckdb import DuckDBResource
from ..resources import SqliteResource

# 1. API INGESTION ASSET


@asset(compute_kind="HackerNews API")
def topstories_api_ingest(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    """Hacker News API se top 100 stories fetch karke DuckDB me save karta hai."""
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

    return MaterializeResult(
        metadata={
            "total_num_records": total_num_records,
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

# 2. SQLITE INGESTION ASSET


@asset(compute_kind="sqlite")
def topstories_sqlite_ingest(context: AssetExecutionContext, sqlite: SqliteResource, duckdb: DuckDBResource) -> MaterializeResult:
    """SQLite database se stories read karke DuckDB me load karta hai."""
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
    """Dono sources ko deduplicate karke common_table me merge karta hai."""
    with duckdb.get_connection() as conn:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'common_table'"
        ).fetchone()[0] > 0

        if not table_exists:
            conn.execute("""
                CREATE TABLE common_table AS
                SELECT * FROM api_ingest
                UNION ALL
                SELECT * FROM sqlite_ingest
            """)
        else:
            conn.execute("""
                INSERT INTO common_table
                SELECT * FROM api_ingest WHERE hashkey NOT IN (SELECT hashkey FROM common_table)
                UNION ALL
                SELECT * FROM sqlite_ingest WHERE hashkey NOT IN (SELECT hashkey FROM common_table)
            """)

        total_num_records = conn.execute(
            "SELECT COUNT(*) FROM common_table").fetchone()[0]

    return MaterializeResult(metadata={"total_num_records": total_num_records})

# 4. TRANSFORMATION (ANALYTICS - TOP WORDS)


@asset(deps=[common_table], compute_kind="duckdb")
def most_frequent_title_words(duckdb: DuckDBResource) -> MaterializeResult:
    """Stopwords filter karke top 25 trending title words calculate karta hai."""
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
    """Streamlit ko update signal bhejta hai."""
    signal_file = "frontend/streamlit_reload_signal.txt"
    with open(signal_file, "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return MaterializeResult(
        metadata={"signal_update_time": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")}
    )
