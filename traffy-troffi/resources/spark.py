import logging
from typing import Dict, List, Optional, Any

from dagster import ConfigurableResource, EnvVar
from pyspark.sql import SparkSession, DataFrameReader, DataFrame

logger = logging.getLogger(__name__)


class SparkSessionResource(ConfigurableResource):
    """Enhanced resource for creating and managing a Spark session with specialized support for PostgreSQL and S3"""
    # Basic Spark configuration
    app_name: str = EnvVar("SPARK_APP_NAME").get_value("Dagster Spark Job")
    master: str = EnvVar("SPARK_MASTER").get_value("local[*]")
    spark_config: Dict[str, str] = {}
    enable_hive_support: bool = EnvVar("SPARK_ENABLE_HIVE").get_value("false").lower() == "true"

    # S3 configuration
    s3_bucket_name: str = EnvVar("S3_BUCKET_NAME").get_value("traffy-troffi")
    s3_endpoint: str = EnvVar("S3_ENDPOINT").get_value("http://localhost:3900")
    s3_access_key: str = EnvVar("S3_ACCESS_KEY").get_value("")
    s3_secret_key: str = EnvVar("S3_SECRET_KEY").get_value("")
    s3_region: str = EnvVar("S3_REGION").get_value("us-east-1")
    s3_path_style_access: bool = True  # Typically needed for non-AWS S3 implementations

    # PostgreSQL configuration
    postgres_user: str = EnvVar("POSTGRES_USER").get_value("postgres")
    postgres_password: str = EnvVar("POSTGRES_PASSWORD").get_value("")
    postgres_db: str = EnvVar("POSTGRES_DB").get_value("traffy-troffi")
    postgres_host: str = EnvVar("POSTGRES_HOST").get_value("localhost")
    postgres_port: str = EnvVar("POSTGRES_PORT").get_value("5432")
    postgres_schema: str = EnvVar("POSTGRES_SCHEMA").get_value("public")

    # Package and driver configuration
    jdbc_driver_path: Optional[str] = EnvVar("JDBC_DRIVER_PATH").get_value(None)
    default_packages: List[str] = ["org.postgresql:postgresql:42.5.4",
                                   "org.apache.hadoop:hadoop-aws:3.3.4",
                                   "com.amazonaws:aws-java-sdk-bundle:1.12.426"
                                   ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._session = None
        self._postgres_jdbc_url = None

    @property
    def postgres_jdbc_url(self) -> str:
        """Generate the JDBC URL for PostgreSQL connections"""
        if not self._postgres_jdbc_url:
            self._postgres_jdbc_url = (
                f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            )
        return self._postgres_jdbc_url

    def get_postgres_properties(self) -> Dict[str, str]:
        """Get the properties for PostgreSQL connections"""
        return {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
            "currentSchema": self.postgres_schema
        }

    def build_session(self) -> SparkSession:
        """Build and return a SparkSession with the configured settings for PostgreSQL and S3"""
        if self._session is not None:
            return self._session

        # Start building the session
        builder = SparkSession.builder.appName(self.app_name).master(self.master)

        # Add Hive support if requested
        if self.enable_hive_support:
            builder = builder.enableHiveSupport()

        # Add all configuration options
        for key, value in self.spark_config.items():
            builder = builder.config(key, value)

        # Configure packages
        if self.default_packages:
            packages = ",".join(self.default_packages)
            builder = builder.config("spark.jars.packages", packages)

        # Add S3 configuration without Hadoop-specific dependencies
        if self.s3_access_key and self.s3_secret_key:
            # Configure S3 with direct configurations
            builder = builder.config("spark.hadoop.fs.s3a.access.key", self.s3_access_key)
            builder = builder.config("spark.hadoop.fs.s3a.secret.key", self.s3_secret_key)
            builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

            # Config for non-AWS S3 (like Garage S3)
            if self.s3_endpoint:
                builder = builder.config("spark.hadoop.fs.s3a.endpoint", self.s3_endpoint)
                builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", self.s3_region)

            # Path style access for non-AWS implementations
            if self.s3_path_style_access:
                builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
                builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                builder = builder.config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")

        # Add JDBC driver if specified
        if self.jdbc_driver_path:
            builder = builder.config("spark.driver.extraClassPath", self.jdbc_driver_path)
            builder = builder.config("spark.executor.extraClassPath", self.jdbc_driver_path)

        # Build the session
        logger.info(f"Building Spark session with app name: {self.app_name}, master: {self.master}")
        self._session = builder.config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
            .getOrCreate()

        return self._session

    def get_session(self) -> SparkSession:
        """Get the current SparkSession or create a new one"""
        return self.build_session()

    # S3 Operations
    def read_s3_csv(self, path: str, **options) -> DataFrame:
        """Read a CSV file from S3 into a Spark DataFrame"""
        s3_path = self._format_s3_path(path)
        session = self.get_session()
        return session.read.options(**options).csv(s3_path)

    def read_s3_parquet(self, path: str, **options) -> Any:
        """Read a Parquet file from S3 into a Spark DataFrame"""
        s3_path = self._format_s3_path(path)
        session = self.get_session()
        return session.read.options(**options).parquet(s3_path)

    def read_s3_json(self, path: str, **options) -> Any:
        """Read a JSON file from S3 into a Spark DataFrame"""
        s3_path = self._format_s3_path(path)
        session = self.get_session()
        return session.read.options(**options).json(s3_path)

    def write_s3_parquet(self, df, path: str, mode: str = "overwrite", **options) -> None:
        """Write a DataFrame to S3 as a Parquet file"""
        s3_path = self._format_s3_path(path)
        df.write.mode(mode).options(**options).parquet(s3_path)

    def write_s3_csv(self, df, path: str, mode: str = "overwrite", **options) -> None:
        """Write a DataFrame to S3 as a CSV file"""
        s3_path = self._format_s3_path(path)
        df.write.mode(mode).options(**options).csv(s3_path)

    def _format_s3_path(self, path: str) -> str:
        """Format a path to use S3A protocol and bucket name if not already specified"""
        if path.startswith("s3a://"):
            return path

        if path.startswith("/"):
            path = path[1:]

        return f"s3a://{self.s3_bucket_name}/{path}"

    # PostgreSQL Operations
    def read_postgres_table(self, table_name: str, schema: str = None, **options) -> Any:
        """Read a PostgreSQL table into a Spark DataFrame"""
        session = self.get_session()
        props = self.get_postgres_properties()

        # Override schema if provided
        if schema:
            table_with_schema = f"{schema}.{table_name}"
        else:
            table_with_schema = f"{self.postgres_schema}.{table_name}"

        return session.read.jdbc(
            url=self.postgres_jdbc_url,
            table=table_with_schema,
            properties=props,
            **options
        )

    def read_postgres_query(self, query: str, **options) -> Any:
        """Read results of a PostgreSQL query into a Spark DataFrame"""
        session = self.get_session()
        props = self.get_postgres_properties()

        # For queries, we need to wrap in parentheses and provide an alias
        query_table = f"({query}) AS query_results"

        return session.read.jdbc(
            url=self.postgres_jdbc_url,
            table=query_table,
            properties=props,
            **options
        )

    def write_postgres_table(self, df, table_name: str, mode: str = "overwrite", schema: str = None, **options) -> None:
        """Write a DataFrame to a PostgreSQL table"""
        props = self.get_postgres_properties()

        # Override schema if provided
        if schema:
            table_with_schema = f"{schema}.{table_name}"
        else:
            table_with_schema = f"{self.postgres_schema}.{table_name}"

        df.write.jdbc(
            url=self.postgres_jdbc_url,
            table=table_with_schema,
            mode=mode,
            properties=props,
            **options
        )

    # Generic file operations
    def read_csv(self, path: str, **options) -> Any:
        """Read a CSV file into a Spark DataFrame"""
        session = self.get_session()
        return session.read.options(**options).csv(path)

    def read_parquet(self, path: str, **options) -> Any:
        """Read a Parquet file into a Spark DataFrame"""
        session = self.get_session()
        return session.read.options(**options).parquet(path)

    def read_json(self, path: str, **options) -> Any:
        """Read a JSON file into a Spark DataFrame"""
        session = self.get_session()
        return session.read.options(**options).json(path)

    def stop_session(self) -> None:
        """Stop the current Spark session if it exists"""
        if self._session is not None:
            self._session.stop()
            self._session = None
            logger.info("Spark session stopped")

    def tear_down(self) -> None:
        """Resource cleanup method called by Dagster"""
        self.stop_session()
