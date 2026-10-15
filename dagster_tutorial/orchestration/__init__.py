from dagster import Definitions, ScheduleDefinition, define_asset_job
from dagster_duckdb import DuckDBResource
from .resources import SqliteResource, PostgresResource
from .assets import (
    topstories_api_ingest,
    topstories_sqlite_ingest,
    common_table,
    most_frequent_title_words,
    streamlit_dashboard,
)

duckdb_resource = DuckDBResource(database="data/dagster_tutorial.duckdb")
sqlite_resource = SqliteResource(database="data/hackernews.sqlite")
postgres_resource = PostgresResource()

api_job = define_asset_job(
    name="api_pipeline_job",
    selection=[topstories_api_ingest, common_table,
               most_frequent_title_words, streamlit_dashboard],
)
api_schedule = ScheduleDefinition(job=api_job, cron_schedule="0 * * * *")

sqlite_job = define_asset_job(
    name="sqlite_pipeline_job",
    selection=[topstories_sqlite_ingest, common_table,
               most_frequent_title_words, streamlit_dashboard],
)
sqlite_schedule = ScheduleDefinition(
    job=sqlite_job, cron_schedule="*/15 * * * *")

defs = Definitions(
    assets=[
        topstories_api_ingest,
        topstories_sqlite_ingest,
        common_table,
        most_frequent_title_words,
        streamlit_dashboard,
    ],
    resources={
        "duckdb": duckdb_resource,
        "sqlite": sqlite_resource,
        "postgres": postgres_resource,
    },
    jobs=[api_job, sqlite_job],
    schedules=[api_schedule, sqlite_schedule],
)
