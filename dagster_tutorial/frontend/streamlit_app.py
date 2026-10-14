import streamlit as st
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import os
import random
import time

st.set_page_config(page_title="Hacker News Trends", layout="wide")


def auto_refresh(interval=2):
    st.html(f"""
        <script>
        setTimeout(function() {{ window.location.reload(); }}, {interval * 1000});
        </script>
    """)


def load_data():
    if not os.path.exists("data/dagster_tutorial.duckdb"):
        return pd.DataFrame()
    conn = duckdb.connect(
        database="data/dagster_tutorial.duckdb", read_only=True)
    try:
        df = pd.read_sql("SELECT * FROM top_words", conn)
    except Exception:
        df = pd.DataFrame()
    finally:
        conn.close()
    return df


def plot_top_words(df, container):
    if df.empty:
        container.info(
            "Abhi data available nahi hai. Dagster me pipeline run karein!")
        return
    fig, ax = plt.subplots(figsize=(10, 5))
    color = (random.random(), random.random(), random.random())
    ax.bar(df["word"], df["count"], color=color)
    plt.xticks(rotation=45, ha="right")
    ax.set_title("Top 25 Words in Hacker News Titles")
    container.pyplot(fig)
    plt.close(fig)


def main():
    st.title("🔥 Hacker News Real-Time Word Trends")
    auto_refresh(2)
    container = st.empty()
    df = load_data()
    plot_top_words(df, container)


if __name__ == "__main__":
    main()
