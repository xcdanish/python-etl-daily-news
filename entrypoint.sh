#!/bin/bash
set -e

# Ensure data and log directories exist
mkdir -p /app/dagster_tutorial/data /app/dagster_tutorial/logs

# Initialize and seed SQLite database if not present on volume mount
if [ ! -f "/app/dagster_tutorial/data/hackernews.sqlite" ]; then
    echo "⚡ Initializing SQLite database (data/hackernews.sqlite)..."
    python docker_setup/init_sqlite.py
fi

# Initialize DuckDB warehouse schema if not present
if [ ! -f "/app/dagster_tutorial/data/dagster_tutorial.duckdb" ]; then
    echo "⚡ Initializing DuckDB database (data/dagster_tutorial.duckdb)..."
    python docker_setup/duckdb_setup.py
fi

exec "$@"
