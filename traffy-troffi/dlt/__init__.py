from dagster import Definitions

from .traffy_fondue import defs as traffy_defs
from .traffy_geojson import defs as geojson_defs
from ..resources.s3 import S3Resource

defs = Definitions.merge(
    Definitions(
        resources={
            "s3": S3Resource()
        }
    )
    , geojson_defs, traffy_defs)
