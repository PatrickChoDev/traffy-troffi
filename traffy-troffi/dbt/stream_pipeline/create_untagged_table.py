import logging
import re
from datetime import datetime
from typing import Dict

from dagster import RetryPolicy, OpExecutionContext, asset

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asset(required_resource_keys={"postgres_credentials"},
       name="traffy_fondue_untagged_postgres_table",
       description="Create a PostgreSQL table for Traffy Fondue untagged data",
       kinds={"postgres", "sql"},
       group_name="traffy_fondue",
       retry_policy=RetryPolicy(max_retries=3, delay=5),
       )
def traffy_fondue_untagged_postgres_table(context: OpExecutionContext) -> Dict[str, str]:
    # SQL to create table and indexes
    create_table_query = """
                         CREATE TABLE IF NOT EXISTS public.traffy_fondue_untagged
                         (
                             ticket_id   VARCHAR(20)      NOT NULL PRIMARY KEY,
                             complaint   TEXT             NOT NULL,
                             timestamp   TIMESTAMP        NOT NULL,
                             image       TEXT             NOT NULL,
                             image_after TEXT,
                             latitude    DOUBLE PRECISION NOT NULL,
                             longitude   DOUBLE PRECISION NOT NULL
                         );

                         ALTER TABLE public.traffy_fondue_untagged
                             OWNER TO postgres;

                         CREATE UNIQUE INDEX IF NOT EXISTS idx_traffy_fondue_untagged_ticket_id ON public.traffy_fondue_untagged (ticket_id);

                         CREATE INDEX IF NOT EXISTS idx_traffy_fondue_untagged_timestamp ON public.traffy_fondue_untagged (timestamp);
                         """

    # Get postgres credentials from context resources
    postgres_creds = context.resources.postgres_credentials

    context.log.info("Connecting to PostgreSQL database")
    connection = postgres_creds.get_connection()
    cursor = connection.cursor()
    try:

        # Split the query by semicolons but preserve semicolons in other contexts
        sql_statements = re.split(r';(?=(?:[^\']*\'[^\']*\')*[^\']*$)', create_table_query)
        context.log.info(f"Executing {len(sql_statements)} DDL statements")

        # Execute each statement
        for sql in sql_statements:
            sql = sql.strip()
            if sql:  # Skip empty statements
                context.log.debug(f"Executing: {sql}")
                cursor.execute(sql)

        # Commit the changes
        connection.commit()
        context.log.info("All DDL statements executed successfully")

        return {
            "success": 'True',
            "timestamp": datetime.now().isoformat(),
            "postgres_table": "traffy_fondue_untagged",
        }

    except Exception as e:
        context.log.error(f"Error executing DDL statements: {str(e)}")
        if connection:
            connection.rollback()
        raise

    finally:
        if connection:
            cursor.close()
            connection.close()
            context.log.info("Database connection closed")
