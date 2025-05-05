from dagster import Definitions

from .traffy_geojson import *

resources = {
    "traffy_api": ApiResource(
        base_url="https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1"
    ),
    "s3": S3Resource()
}

# Define Dagster definitions
defs = Definitions(
    assets=[traffy_geojson_to_s3],
    resources=resources,
    schedules=[traffy_schedule],
    jobs=[traffy_data_job]
)