from dagster import ConfigurableResource
import sqlite3
import psycopg2


class SqliteResource(ConfigurableResource):
    database: str

    def get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.database)


class PostgresResource(ConfigurableResource):
    host: str = "postgres-db"
    port: int = 5432
    database: str = "hackernews_db"
    username: str = "postgres"
    password: str = "postgrespassword"

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.username,
            password=self.password,
        )
