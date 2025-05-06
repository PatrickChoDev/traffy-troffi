import json
from datetime import datetime
from typing import Dict, Any

from dagster import (
    asset,
    AssetExecutionContext,
    ScheduleDefinition,
    define_asset_job,
    AssetIn,
    Definitions
)

from ..resources.api import ApiResource


# First asset: Fetch resources from API
@asset(required_resource_keys={"traffy_api"},
       group_name="traffy_geojson",
     compute_kind="json",
       description="Fetches GeoJSON resources from Traffy API")
def traffy_geojson_raw(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Asset that fetches GeoJSON resources from Traffy API.
    """
    # Create a timestamp for metadata
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Log start of a process
    context.log.info(f"Starting Traffy GeoJSON resources fetch at {timestamp}")

    # Fetch resources from API
    api_data = context.resources.traffy_api.fetch_json()

    # Validate the resources has an expected structure
    if not isinstance(api_data, dict):
        raise ValueError(f"API returned unexpected resources type: {type(api_data)}")

    # Check if there are any resources
    context.log.info(f"Received {len(json.dumps(api_data))} bytes of GeoJSON resources")

    # Return the resources and metadata
    return {
        "resources": api_data,
        "timestamp": timestamp,
        "data_size_bytes": len(json.dumps(api_data)),
        "features_count": len(api_data.get("features", [])) if "features" in api_data else None
    }


# Second asset: Store resources in S3
@asset(
    group_name="traffy_geojson",
    required_resource_keys={"s3"},
    compute_kind="s3",
    ins={"traffy_data": AssetIn(key="traffy_geojson_raw")}
)
def traffy_geojson_s3(context: AssetExecutionContext, traffy_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asset that stores GeoJSON resources in S3.
    """
    # Extract resources and metadata
    api_data = traffy_data["resources"]
    timestamp = traffy_data["timestamp"]

    # Generate S3 key with timestamp
    s3_key = f"traffy/geojson/traffy_geojson_{timestamp}.json"

    # Upload to S3
    upload_result = context.resources.s3.upload_json(s3_key, api_data)

    # Also upload to a "latest" version
    latest_key = "traffy/geojson/latest.json"
    context.resources.s3.upload_json(latest_key, api_data)

    # Return metadata about the S3 storage
    return {
        "s3_location": upload_result,
        "timestamp": timestamp,
        "data_size_bytes": traffy_data["data_size_bytes"],
        "features_count": traffy_data["features_count"]
    }


# Define a job that includes both assets
traffy_data_job = define_asset_job(
    "ingest_traffy_geojson_job",
    description="Job that downloads GeoJSON resources from Traffy API and uploads them to S3",
    selection=[traffy_geojson_raw, traffy_geojson_s3]
)

# Schedule definition remains the same
traffy_schedule = ScheduleDefinition(
    job=traffy_data_job,
    cron_schedule="*/5 * * * *"  # Every 5 minutes
)

resources = {
    "traffy_api": ApiResource(
        base_url="https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1"
    ),
}

# Define Dagster definitions
geojson_defs = Definitions(
    assets=[traffy_geojson_raw, traffy_geojson_s3],
    resources=resources,
    schedules=[traffy_schedule],
    jobs=[traffy_data_job]
)
