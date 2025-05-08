import logging
from typing import Optional, Dict, Any, List

import psycopg2
import psycopg2.extras
from dagster import ConfigurableResource, EnvVar

logger = logging.getLogger(__name__)


class PostgresSQLResource(ConfigurableResource):
    """Resource for interacting with PostgresSQL database"""
    # Configuration with environment variable defaults
    host: str = EnvVar("POSTGRES_HOST")
    port: int = EnvVar("POSTGRES_PORT")
    database: str = EnvVar("POSTGRES_DB")
    username: str = EnvVar("POSTGRES_USER")
    password: str = EnvVar("POSTGRES_PASSWORD")
    schema: str = EnvVar("POSTGRES_SCHEMA")

    def _get_connection(self):
        """Create and return a database connection"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgresSQL: {e}")
            raise

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as a list of dictionaries"""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                if cursor.description:  # Check if query returns data
                    return cursor.fetchall()
                conn.commit()
                return []

    def insert_data(self, table_name: str, data: Dict[str, Any]) -> int:
        """Insert a single row into a specified table"""
        columns = list(data.keys())
        placeholders = [f"%({col})s" for col in columns]

        query = f"""
        INSERT INTO {self.schema}.{table_name} 
        ({', '.join(columns)}) 
        VALUES ({', '.join(placeholders)})
        RETURNING id
        """

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, data)
                result = cursor.fetchone()
                conn.commit()
                return result[0] if result else None

    def bulk_insert(self, table_name: str, data_list: List[Dict[str, Any]]) -> int:
        """Insert multiple rows into specified table"""
        if not data_list:
            return 0

        # Assume all dictionaries have the same keys
        columns = list(data_list[0].keys())
        placeholders = [f"%({col})s" for col in columns]

        query = f"""
        INSERT INTO {self.schema}.{table_name} 
        ({', '.join(columns)}) 
        VALUES ({', '.join(placeholders)})
        """

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, query, data_list)
                conn.commit()
                return cursor.rowcount

    def create_table(self, table_name: str, columns_def: Dict[str, str],
                     primary_key: Optional[str] = None, if_not_exists: bool = True) -> None:
        """
        Create a table with the specified columns

        Args:
            table_name: Name of the table to create
            columns_def: Dictionary mapping column names to their SQL types
            primary_key: Column to use as primary key (if any)
            if_not_exists: Whether to use IF NOT EXISTS clause
        """
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        # Build column definitions
        column_defs = [f"{name} {data_type}" for name, data_type in columns_def.items()]

        # Add primary key if specified
        if primary_key and primary_key in columns_def:
            column_defs = [f"{col} PRIMARY KEY" if col == primary_key else col for col in column_defs]

        query = f"""
        CREATE TABLE {exists_clause}{self.schema}.{table_name} (
            {', '.join(column_defs)}
        )
        """

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        query = """
                SELECT EXISTS (SELECT
                               FROM information_schema.tables
                               WHERE table_schema = %s
                                 AND table_name = %s) \
                """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (self.schema, table_name))
                return cursor.fetchone()[0]

    def execute_transaction(self, queries: List[Dict[str, Any]]) -> None:
        """
        Execute multiple queries in a transaction

        Args:
            queries: List of dictionaries with keys 'query' and 'params'
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                for q in queries:
                    cursor.execute(q['query'], q.get('params'))
                conn.commit()
