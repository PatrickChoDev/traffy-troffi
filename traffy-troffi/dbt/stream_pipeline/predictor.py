from datetime import datetime
from typing import Dict, Any

import numpy as np
import torch
from dagster import (
    asset,
    AssetExecutionContext,
    AssetIn,
    Definitions,
    resource,
)
from pyspark.sql import DataFrame
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from ...resources.spark import SparkSessionResource


@resource(description="Spark session resource.")
def spark_session():
    """Dagster resource for SparkSession"""
    spark_resource = SparkSessionResource(
        app_name="ml_predictor_spark",
    )
    try:
        yield spark_resource
    finally:
        spark_resource.tear_down()


@asset(description="Extract data from PostgreSQL database.", kinds={"spark", "postgres"}, group_name="traffy_fondue", )
def new_data_traffy_stream(context: AssetExecutionContext, spark_predict_pipeline: SparkSessionResource,
                           traffy_fondue_unpredicted_postgres_table: Dict[str, str]) -> Dict[str, str]:
    """Extract data from PostgreSQL database."""
    context.log.info("Loading data from PostgreSQL")
    context.log.info(f"Postgres table: {traffy_fondue_unpredicted_postgres_table['postgres_table']}")
    context.log.info(f"Postgres URL: {spark_predict_pipeline.postgres_jdbc_url}")
    context.log.info(f"Postgres properties: {spark_predict_pipeline.get_postgres_properties()}")
    df = spark_predict_pipeline.get_session().read.jdbc(
        table=traffy_fondue_unpredicted_postgres_table['postgres_table'],
        url=spark_predict_pipeline.postgres_jdbc_url,
        properties=spark_predict_pipeline.get_postgres_properties())
    output_path = f"s3a://{spark_predict_pipeline.s3_bucket_name}/spark/traffy_predictions/new_data_{datetime.now().isoformat()}"
    df.write.mode("overwrite").parquet(output_path)
    empty_df = spark_predict_pipeline.get_session().createDataFrame([], df.schema)
    empty_df.write.mode("overwrite").jdbc(table=traffy_fondue_unpredicted_postgres_table['postgres_table'],
                                          url=spark_predict_pipeline.postgres_jdbc_url,
                                          properties=spark_predict_pipeline.get_postgres_properties())
    context.log.info(f"Loaded {df.count()} rows from traffy_fondue table")
    return {
        "output_path": output_path,
    }


@asset(kinds={"pytorch"}, group_name="traffy_fondue")
def ml_model() -> dict:
    """Load the ML model for predictions."""
    checkpoint = "phor2547/final-dsde-type"
    tokenizer = AutoTokenizer.from_pretrained(checkpoint)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    model = AutoModelForSequenceClassification.from_pretrained(
        checkpoint,
        problem_type="multi_label_classification"
    ).to(device)

    return {"tokenizer": tokenizer, "model": model, "device": device}


@asset(ins={"new_data": AssetIn("new_data_traffy_stream"), "model": AssetIn("ml_model")},
       kinds={"spark", "pytorch"}, group_name="traffy_fondue")
def predictions_data(
        context: AssetExecutionContext,
        spark_predict_pipeline: SparkSessionResource,
        new_data: Dict[str, str],
        model: dict
) -> Dict[str, str]:
    """Generate predictions for the filtered data."""
    spark_session = spark_predict_pipeline.get_session()

    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, DoubleType, FloatType
    from pyspark.sql import functions as F

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

    context.log.info("Generating predictions")
    new_data = spark_session.read.parquet(new_data["output_path"])
    context.log.info(f"Loaded {new_data.count()} rows from new_data")

    # First, load the categories mapping from the reference dataset
    context.log.info("Loading category mapping from reference data")
    categories_df = spark_session.read.csv("s3a://traffy-troffi/traffy/fondue/traffy_fondue_latest.csv",
                                           header=True,
                                           multiLine=True,
                                           mode="DROPMALFORMED")

    # Extract just the category information to create our mapping
    mapping_df = categories_df.select(
        F.expr("cast(split(coords, ',')[1] as double)").alias("latitude"),
        F.expr("cast(split(coords, ',')[0] as double)").alias("longitude"),
        categories_df["type"]
    ).withColumn(
        "extracted_content",
        F.regexp_extract(F.col("type"), "\\{(.+)\\}", 1)
    ).withColumn(
        "categories",
        F.split(F.col("extracted_content"), ",")
    ).withColumn(
        "categories",
        F.transform(F.col("categories"), lambda x: F.trim(x))
    ).withColumn(
        "categories",
        F.expr("filter(categories, x -> x != '')")
    ).drop("extracted_content", "type")

    # Extract unique categories and create an indexed version
    # First, explode the categories array to get individual categories
    exploded_categories = mapping_df.select(F.explode("categories").alias("category")).distinct()

    # Create an IndexingModel using StringIndexer (reverse of IndexToString)
    from pyspark.ml.feature import StringIndexer
    indexer = StringIndexer(inputCol="category", outputCol="category_idx")
    indexer_model = indexer.fit(exploded_categories)

    # Get the labels in the order they were indexed
    category_labels = indexer_model.labels
    context.log.info(f"Created mapping for {len(category_labels)} categories")

    # Check if there are any rows in the dataset
    if new_data.count() == 0:
        context.log.info("No data to process. Returning empty predictions.")
        output_path = f"s3a://{spark_predict_pipeline.s3_bucket_name}/spark/traffy_predictions/predictions_{datetime.now().isoformat()}"
        # Create an empty DataFrame with the expected schema
        empty_df = spark_session.createDataFrame([], complaint_schema)
        empty_df.write.mode("overwrite").parquet(output_path)
        return {
            "output_path": output_path
        }

    # Collect the data to process with PyTorch
    row_ids = [row.ticket_id for row in new_data.select("ticket_id").collect()]
    context.log.info(f"Collected {len(row_ids)} row IDs")
    complaints = [row.complaint for row in new_data.select("complaint").collect()]
    context.log.info(f"Collected {len(complaints)} complaints")

    # Handle case where we have rows but no complaints
    if len(complaints) == 0:
        context.log.info("No complaints to process. Creating empty predictions.")
        predictions_df = spark_session.createDataFrame(
            [(id, [], []) for id in row_ids],
            ["ticket_id", "categories_idx", "categories"]
        )
    else:
        # Prepare inputs for the model
        tokenizer = model["tokenizer"]
        ml_model = model["model"]
        device = model["device"]

        inputs = tokenizer(
            complaints,
            padding=True,
            truncation=True,
            return_tensors="pt"
        ).to(device)

        # Generate predictions
        ml_model.eval()
        encoded_predictions = []
        with torch.no_grad():
            outputs = ml_model(**inputs)
            logits = outputs.logits
            probs = torch.sigmoid(logits).cpu().numpy()
            preds = (probs >= 0.5).astype(int)

            for pred_row in preds:
                positive_indices = np.where(pred_row == 1)[0]
                encoded_predictions.append(positive_indices.tolist())
        context.log.info(f"Generated {len(encoded_predictions)} predictions")

        # Map indices to category strings
        category_predictions = []
        for indices in encoded_predictions:
            # Map each index to its corresponding category name
            # Handle out-of-bounds indices by filtering them out
            categories = [category_labels[idx] for idx in indices if 0 <= idx < len(category_labels)]
            category_predictions.append(categories)

        # Create DataFrame with predictions and mapped categories
        predictions_df = spark_session.createDataFrame(
            [(id, pred, cats) for id, pred, cats in
             zip(row_ids, encoded_predictions, category_predictions)],
            ["ticket_id", "categories_idx", "categories"]
        )

    context.log.info(f"Created predictions DataFrame with {predictions_df.count()} rows")

    # Join with original data
    result_df = new_data.join(
        predictions_df,
        on="ticket_id",
        how="left"
    )

    # Cache the result as a temporary artifact
    result_df.cache()

    context.log.info(f"Generated predictions for {result_df.count()} rows")
    output_path = f"s3a://{spark_predict_pipeline.s3_bucket_name}/spark/traffy_predictions/predictions_{datetime.now().isoformat()}"
    result_df.write.mode("overwrite").parquet(output_path)
    return {
        "output_path": output_path
    }


@asset(ins={"result_data": AssetIn("predictions_data")}, group_name="traffy_fondue", )
def store_predicted_traffy_parquet(context: AssetExecutionContext, spark_predict_pipeline: SparkSessionResource,
                                   result_data: Dict[str, str]) -> Dict[str, str]:
    """Save results to a temporary location that can be accessed by the next job."""

    output_path = f"s3a://{spark_predict_pipeline.s3_bucket_name}/spark/traffy_predictions/prediction_{datetime.now().isoformat()}"
    result_data = context.resources.spark_predict_pipeline.get_session().read.parquet(result_data["output_path"])
    result_data.write.mode("overwrite").parquet(output_path)
    context.log.info(f"Saved prediction results to {output_path}")
    return {
        "output_path": output_path
    }


@asset(
    name="store_predicted_traffy_stream_postgres",
    description="Store predicted data in PostgreSQL with timestamp",
    group_name="traffy_fondue",
    ins={
        "save_result": AssetIn("store_predicted_traffy_parquet"),
        "create_traffy_fondue_postgres_table": AssetIn("create_traffy_fondue_postgres_table")
    },
)
def store_predicted_traffy_stream_postgres(
        context: AssetExecutionContext,
        spark_predict_pipeline: SparkSessionResource,
        save_result: Dict[str, str],  # Changed to accept from save_result asset
        create_traffy_fondue_postgres_table: Dict[str, str]
) -> Dict[str, Any]:
    """
    Stores the processed data in PostgreSQL and returns timestamp information
    """
    context.log.info("Storing processed data in PostgreSQL")
    spark_session = spark_predict_pipeline.get_session()

    # Get the path from save_result
    output_path = save_result["output_path"]
    context.log.info(f"Reading data from: {output_path}")

    # Get table name from create_traffy_fondue_postgres_table
    postgres_table = create_traffy_fondue_postgres_table['postgres_table']
    context.log.info(f"Target PostgreSQL table: {postgres_table}")

    # Read the parquet file from S3
    df = spark_session.read.parquet(output_path)

    # Write to PostgreSQL
    df.write.mode("append").jdbc(
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
        "rows_written": df.count(),
        "source_path": output_path,
        "target_table": postgres_table
    }


# Define Dagster resources and jobs
ml_predictor_defs = Definitions(
    assets=[new_data_traffy_stream, ml_model, predictions_data, store_predicted_traffy_parquet,
            store_predicted_traffy_stream_postgres],
    resources={
        "spark_predict_pipeline": spark_session.configured({"app_name": "Traffy Data Pipeline"})
    }
)
