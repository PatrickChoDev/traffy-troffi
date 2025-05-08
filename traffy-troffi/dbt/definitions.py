from dagster import Definitions

from .traffy_fondue import traffy_defs
from .fondue_pipelines import fondue_pipelines_defs
from .stream_pipeline import stream_pipeline_defs


def get_dbt_definitions():
    return Definitions.merge(
        traffy_defs,
        fondue_pipelines_defs,
        stream_pipeline_defs,
    )
