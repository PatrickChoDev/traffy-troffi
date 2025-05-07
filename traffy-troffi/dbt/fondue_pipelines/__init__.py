import psycopg2
from dagster import Definitions, job, ConfigurableResource, EnvVar

from .create_traffy_fondue_table import create_traffy_fondue_postgres_table
from .process_complaint_traffy_fondue import *


@job
def init_traffy_fondue_postgres_table_jobs():
    create_traffy_fondue_postgres_table()
    # clean_traffy_fondue_data()
    # write_traffy_fondue_parquet()
    # store_traffy_fondue_postgres()


class PostgresCredentialsResource(ConfigurableResource):
    username: str
    password: str
    host: str
    port: int
    database: str
    schema: str = "public"
    application_name: str = "traffy_fondue_postgres_resource"

    def get_connection(self):
        """Create and return a psycopg2 connection."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.username,
            password=self.password,
            application_name=self.application_name
        )

    @property
    def connection_string(self):
        """Return connection string for psycopg2."""
        return f"host={self.host} port={self.port} dbname={self.database} user={self.username} password={self.password}"


fondue_pipelines_defs = Definitions(
    jobs=[init_traffy_fondue_postgres_table_jobs],
    assets=[create_traffy_fondue_postgres_table,
            processed_traffy_fondue_data,
            write_processed_traffy_fondue_parquet,
            store_processed_traffy_fondue_postgres
            ],
    resources={
        "postgres_credentials": PostgresCredentialsResource(
            username=EnvVar("POSTGRES_USER").get_value("postgres"),
            password=EnvVar("POSTGRES_PASSWORD").get_value("postgres"),
            host=EnvVar("POSTGRES_HOST").get_value("localhost"),
            port=int(EnvVar("POSTGRES_PORT").get_value("5432")),
            database=EnvVar("POSTGRES_DB").get_value("traffy")
        )
    }
)
