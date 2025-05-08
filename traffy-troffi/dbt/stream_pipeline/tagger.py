import logging
from datetime import datetime
from typing import Dict, Any

import geopandas as gpd
from dagster import asset, resource, Definitions, AssetExecutionContext, AssetIn
import pandas as pd
from shapely.geometry import Point

from ...resources.s3 import S3Resource
from ...resources.spark import SparkSessionResource

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@resource
def tagger_spark_session_resource(context):
    """Resource that provides access to an existing SparkSession"""
    spark = SparkSessionResource(
        app_name="traffy_stream_tagger_pipeline",
    )

    try:
        yield spark
    finally:
        pass


@asset(required_resource_keys={"tagger_spark_session_resource"}, kinds={"spark", "postgres"},
       group_name="traffy_fondue")
def untagged_traffy_stream_data(context, traffy_fondue_untagged_postgres_table) -> Dict[str, str]:
    """Load raw data from PostgreSQL using Spark JDBC"""
    spark: SparkSessionResource = context.resources.tagger_spark_session_resource

    # Load data from PostgreSQL
    df = spark.get_session().read.jdbc(table=traffy_fondue_untagged_postgres_table['postgres_table'],
                                       url=spark.postgres_jdbc_url,
                                       properties=spark.get_postgres_properties())
    empty_df = spark.get_session().createDataFrame([], df.schema)
    empty_df.write.mode("overwrite").jdbc(table=traffy_fondue_untagged_postgres_table['postgres_table'],
                                          url=spark.postgres_jdbc_url,
                                          properties=spark.get_postgres_properties())
    output_path = f"s3a://{spark.s3_bucket_name}/spark/traffy_predictions/untagged_{datetime.now().isoformat()}"
    df.write.mode("overwrite").parquet(output_path)

    context.log.info(f"Loaded {df.count()} rows from traffy_fondue table")
    return {
        "output_path": output_path,
    }


@asset(required_resource_keys={"tagger_s3_session_resource"}, kinds={"s3"}, group_name="traffy_fondue")
def bangkok_geospatial_data(context) -> Dict[str, str]:
    """Load geospatial data from GeoJSON file"""
    s3: S3Resource = context.resources.tagger_s3_session_resource
    file = s3.download_file("traffy/data/subdistricts.geojson")
    output_path = f"/tmp/subdistricts_{datetime.now().strftime('%Y%m%d%H%M%S')}.geojson"
    with open(output_path, "wb") as f:
        f.write(file.getvalue())
    return {
        "output_path": output_path,
    }


@asset(required_resource_keys={"tagger_spark_session_resource"}, kinds={"spark", "geojson"}, group_name="traffy_fondue")
def geospatial_processed_data(context, untagged_traffy_stream_data: Dict[str, str],
                              bangkok_geospatial_data: Dict[str, str]):
    """Process geospatial data to identify subdistricts and districts"""
    spark = context.resources.tagger_spark_session_resource.get_session()

    # Load data using Spark but convert to pandas DataFrame
    context.log.info("Loading untagged data from parquet")
    spark_df = spark.read.parquet(untagged_traffy_stream_data["output_path"])
    pandas_df = spark_df.toPandas()

    # Load geospatial data with geopandas
    context.log.info("Loading geospatial data from GeoJSON file")
    regions_gdf = gpd.read_file(bangkok_geospatial_data["output_path"])

    # Function to determine which region a point belongs to
    def get_location_info(row):
        if pd.isna(row['latitude']) or pd.isna(row['longitude']):
            return pd.Series([None, None], index=['subdistrict', 'district'])

        try:
            point = Point(float(row['longitude']), float(row['latitude']))

            for _, region in regions_gdf.iterrows():
                if region.geometry.contains(point):
                    subdistrict = region['SNAME'].replace("แขวง", "").strip()
                    district = region['DNAME'].replace("เขต", "").strip()
                    return pd.Series([subdistrict, district], index=['subdistrict', 'district'])

            return pd.Series([None, None], index=['subdistrict', 'district'])
        except Exception as e:
            context.log.error(f"Error in get_location_info: {str(e)}")
            return pd.Series([None, None], index=['subdistrict', 'district'])

    # Apply the function to each row and add the new columns
    context.log.info("Performing geospatial processing with pandas")
    location_info = pandas_df.apply(get_location_info, axis=1)
    pandas_df[['subdistrict', 'district']] = location_info

    # Filter rows with valid subdistrict and district
    final_df = pandas_df[pandas_df['subdistrict'].notna() & pandas_df['district'].notna()]

    # Convert back to Spark DataFrame for saving
    context.log.info("Converting processed data back to Spark DataFrame")
    final_spark_df = spark.createDataFrame(final_df)

    # Save the result
    context.log.info("Completed geospatial processing")
    output_path = f"s3a://{context.resources.tagger_spark_session_resource.s3_bucket_name}/spark/traffy_predictions/geoprocessed_{datetime.now().isoformat()}"
    final_spark_df.write.mode("overwrite").parquet(output_path)

    return {
        "output_path": output_path
    }


@asset(
    name="store_tagged_traffy_stream_postgres",
    description="Store predicted data in PostgreSQL with timestamp",
    group_name="traffy_fondue",
    ins={
        "processed": AssetIn("geospatial_processed_data"),
        "traffy_fondue_unpredicted_postgres_table": AssetIn("traffy_fondue_unpredicted_postgres_table")
    },
)
def store_tagged_traffy_stream_postgres(
        context: AssetExecutionContext,
        spark_predict_pipeline: SparkSessionResource,
        processed: Dict[str, str],  # Changed to accept from save_result asset
        traffy_fondue_unpredicted_postgres_table: Dict[str, str]
) -> Dict[str, Any]:
    """
    Stores the processed data in PostgreSQL and returns timestamp information
    """
    context.log.info("Storing processed data in PostgreSQL")

    # Get table name from create_traffy_fondue_postgres_table
    postgres_table = traffy_fondue_unpredicted_postgres_table['postgres_table']
    context.log.info(f"Target PostgreSQL table: {postgres_table}")
    # Write to PostgreSQL
    processed = spark_predict_pipeline.get_session().read.parquet(processed["output_path"])
    processed.write.mode("append").jdbc(
        url=spark_predict_pipeline.postgres_jdbc_url,
        table=postgres_table,
        properties={
            "user": spark_predict_pipeline.postgres_user,
            "password": spark_predict_pipeline.postgres_password,
            "driver": "org.postgresql.Driver",
            "currentSchema": spark_predict_pipeline.postgres_schema,
        }
    )

    return {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "rows_written": processed.count(),
        "target_table": postgres_table
    }


# Create Dagster definitions
tagger_defs = Definitions(
    assets=[untagged_traffy_stream_data, geospatial_processed_data, bangkok_geospatial_data,
            store_tagged_traffy_stream_postgres],
    resources={
        "tagger_spark_session_resource": tagger_spark_session_resource,
        "tagger_s3_session_resource": S3Resource(),
    },
)
