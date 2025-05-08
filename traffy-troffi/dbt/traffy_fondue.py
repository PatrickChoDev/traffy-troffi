import logging

from dagster import (
    asset, resource, Definitions, define_asset_job
)

from ..resources.spark import SparkSessionResource

logger = logging.getLogger(__name__)


@resource
def traffy_fondue_build_parquet_spark_resource(context) -> SparkSessionResource:
    """Resource for Spark session"""
    return SparkSessionResource(
        app_name="traffy_fondue_build_parquet_spark_resource"
    )


# Assets
@asset(
    name="traffy_fondue_parquet",
    description="generate parquet file from Traffy Fondue dataset",
    group_name="traffy_fondue",
    kinds={"python", "s3", "spark"}
)
def traffy_fondue_parquet(context, spark: SparkSessionResource):
    """Asset that loads CSV from S3 and writes to parquet format"""

    s3_path = "s3a://traffy-troffi/traffy/fondue/traffy_fondue_latest.csv"
    output_path = "s3a://traffy-troffi/spark/traffy_fondue.parquet"
    
    context.log.info('S3: {}'.format([spark.s3_access_key, spark.s3_secret_key]))

    context.log.info(f"Reading CSV file from {s3_path}")
    df = spark.read_s3_csv(s3_path, header=True, inferSchema=True)

    context.log.info(f"Writing parquet file to {output_path}")
    df.write.parquet(output_path, mode="overwrite")

    context.log.info("Completed processing")

    # Return metadata instead of DataFrame
    return {
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns,
        "output_path": output_path
    }


__traffy_fondue_parquet_asset_job = define_asset_job(
    name="traffy_fondue_parquet_asset_job",
    description="Job to build parquet files from Traffy Fondue dataset",
    selection=[traffy_fondue_parquet],
    run_tags={"kind": "spark"},
)

traffy_defs = Definitions(
    assets=[traffy_fondue_parquet],
    resources={"spark": traffy_fondue_build_parquet_spark_resource},
)
