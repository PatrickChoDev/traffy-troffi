from dagster import Definitions

from .traffy_fondue import traffy_defs
from .traffy_geojson import geojson_defs
from .traffy_process_raw import process_raw_defs
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
        process_raw_defs
    )
