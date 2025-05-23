from dagster import Definitions

from .traffy_fondue import traffy_defs
from .traffy_ml import traffy_model_defs
from .traffy_geojson import geojson_defs
from .traffy_web_scrap import process_web_scrap_defs
from ..resources.s3 import S3Resource


def get_dlt_definitions():
    return Definitions.merge(
        Definitions(
            resources={
                "s3": S3Resource()
            }
        ),
        geojson_defs,
        traffy_defs,
        process_web_scrap_defs,
        traffy_model_defs
    )
