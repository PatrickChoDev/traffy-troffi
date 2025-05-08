from dagster import Definitions

from .traffy_fondue import traffy_defs
from .traffy_ml import traffy_model_defs
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
<<<<<<< HEAD
        process_raw_defs
=======
        traffy_model_defs
>>>>>>> 10d9755c8a92a47d3b2a3d45c568eece208aedc3
    )
