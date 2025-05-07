import logging
from datetime import datetime
from typing import Dict

from dagster import asset, AssetExecutionContext
from pyspark.ml.feature import StringIndexer
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, DoubleType, FloatType

from ...resources.spark import SparkSessionResource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asset(
    name="processed_traffy_fondue_data",
    description="Process and clean complaint data from Traffy Fondue dataset",
    group_name="traffy_fondue",
    kinds={"spark"},
)
def processed_traffy_fondue_data(context: AssetExecutionContext, spark: SparkSessionResource,
                                 traffy_fondue_parquet: Dict[str, str]) -> DataFrame:
    """
    Loads raw data from S3, processes it, and returns a cleaned DataFrame
    """
    spark = spark.get_session()
    df = spark.read.parquet(traffy_fondue_parquet['output_path'])

    processed_df = df.filter((F.col('state') == 'เสร็จสิ้น') & (F.col("photo_after").isNotNull())).select(
        df['ticket_id'].alias("ticket_id"),
        df["comment"].alias("complaint"),
        F.to_timestamp(F.regexp_replace(F.col("last_activity").cast("string"), "\\+\\d{2}$", "")).alias("timestamp"),
        df["photo"].alias("image"),
        df["photo_after"].alias("image_after"),
        df["type"],
        # Parse coordinates to separate lat/long fields
        F.expr("cast(split(coords, ',')[1] as double)").alias("latitude"),
        F.expr("cast(split(coords, ',')[0] as double)").alias("longitude"),
        df["district"].alias("district"),
        df["subdistrict"].alias("subdistrict"),
    ).withColumn(
        "extracted_content",
        F.regexp_extract(F.col("type"), "\\{(.+)\\}", 1)
    ).withColumn(
        "categories",
        F.split(F.col("extracted_content"), ",")
    ).withColumn(
        "categories",
        F.transform(F.col("categories"), lambda x: F.trim(x))
        # Filter out empty strings
    ).withColumn(
        "categories",
        F.expr("filter(categories, x -> x != '')")
    ).drop("extracted_content", "type")

    # Add unique ID to original data
    df_with_id = processed_df.withColumn("row_id", monotonically_increasing_id())

    # Skip rows with empty categories arrays
    df_with_categories = df_with_id.filter(F.size("categories") > 0)

    # Explode array with ID
    exploded = df_with_categories.select("row_id", "*", F.explode("categories").alias("category_item"))

    # Filter out any remaining empty strings or nulls in category_item
    exploded = exploded.filter((F.col("category_item") != "") & (F.col("category_item").isNotNull()))

    # Fit indexer on unique categories only
    indexer = StringIndexer().setInputCol("category_item").setOutputCol("category_idx").fit(
        exploded.select("category_item").distinct())

    # Apply indexer
    indexed = indexer.transform(exploded)

    # Group back by ID to preserve the original structure while drop uncategorized
    result_df = indexed.groupBy("row_id").agg(
        *[F.first(c).alias(c) for c in df_with_id.columns if c != "categories" and c != "row_id"],
        F.collect_list("category_item").alias("categories"),
        F.collect_list("category_idx").alias("categories_idx")
    ).drop("row_id")

    complaint_schema = StructType([
        StructField("ticket_id", StringType(), nullable=False, metadata={"description": "Ticket ID"}),
        StructField("complaint", StringType(), nullable=False, metadata={"description": "Complaint text"}),
        StructField("timestamp", TimestampType(), nullable=False, metadata={"description": "Timestamp of complaint"}),
        StructField("image", StringType(), nullable=False, metadata={"description": "Image URL"}),
        StructField("image_after", StringType(), nullable=False,
                    metadata={"description": "Image URL after processing"}),
        StructField("latitude", DoubleType(), nullable=False, metadata={"description": "Latitude"}),
        StructField("longitude", DoubleType(), nullable=False, metadata={"description": "Longitude"}),
        StructField("district", StringType(), nullable=False,
                    metadata={"description": "District", "ml_attr_type": "nominal"}),
        StructField("subdistrict", StringType(), nullable=False,
                    metadata={"description": "Subdistrict", "ml_attr_type": "nominal"}),
        StructField("categories", ArrayType(StringType()), nullable=False, metadata={"description": "Categories"}),
        StructField("categories_idx", ArrayType(FloatType()), nullable=False,
                    metadata={"description": "Categories indices"}),
    ])

    # Step 1: Get list of non-nullable columns from your DataFrame schema
    non_nullable_columns = [field.name for field in complaint_schema.fields if not field.nullable]

    # Step 2: Print the non-nullable columns for verification
    context.log.debug(f"Non-nullable columns: {non_nullable_columns}")

    # Step 3: Create a filter condition to keep only rows without nulls in these columns
    if non_nullable_columns:
        filter_condition = " AND ".join([f"{col} IS NOT NULL" for col in non_nullable_columns])

        # Step 4: Apply the filter to drop rows with nulls in non-nullable columns
        cleaned_df = result_df.filter(filter_condition)

        # Step 5: Check how many rows were dropped
        original_count = result_df.count()
        cleaned_count = cleaned_df.count()
        context.log.debug(f"Original row count: {original_count}")
        context.log.debug(f"Cleaned row count: {cleaned_count}")
        context.log.debug(f"Dropped {original_count - cleaned_count} rows with nulls in non-nullable columns")
    else:
        context.log.debug("No non-nullable columns found in schema")
        cleaned_df = result_df

    # Create the proper schema DataFrame
    structure_df = spark.createDataFrame(cleaned_df.rdd, schema=complaint_schema)
    return structure_df


@asset(
    name="write_processed_traffy_fondue_parquet",
    description="Write processed Traffy Fondue data to S3 in parquet format",
    group_name="traffy_fondue",
    kinds={"spark", "s3"},
)
def write_processed_traffy_fondue_parquet(
        context: AssetExecutionContext,
        spark: SparkSessionResource,
        processed_traffy_fondue_data: DataFrame
) -> Dict[str, str]:
    """
    Writes the cleaned DataFrame to S3 in parquet format
    """
    parquet_path = "s3a://traffy-troffi/spark/traffy/fondue"
    processed_traffy_fondue_data.write.mode("overwrite").parquet(parquet_path)

    return {
        "success": "True",
        "timestamp": datetime.now().isoformat(),
        "parquet_path": parquet_path,
    }


@asset(
    name="store_processed_traffy_fondue_postgres",
    description="Store Traffy Fondue data in PostgreSQL with timestamp",
    group_name="traffy_fondue",
    kinds={"spark", "postgres"},
)
def store_processed_traffy_fondue_postgres(
        context: AssetExecutionContext,
        processed_traffy_fondue_data: DataFrame,
        create_traffy_fondue_postgres_table: Dict[str, str]
) -> Dict[str, str]:
    """
    Stores the processed data in PostgreSQL and returns timestamp information
    """
    processed_traffy_fondue_data.write.mode("append").jdbc(
        table=create_traffy_fondue_postgres_table['postgres_table'],
        url="jdbc:postgresql://localhost:5432/traffy-troffi",
        properties={
            "user": "postgres",
            "password": "troffi",
            "driver": "org.postgresql.Driver",
            "currentSchema": "public"
        }
    )

    return {
        "success": "True",
        "timestamp": datetime.now().isoformat(),
        "table": create_traffy_fondue_postgres_table['postgres_table'],
    }
