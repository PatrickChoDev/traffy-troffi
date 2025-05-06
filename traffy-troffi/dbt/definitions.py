from dagster import Definitions

from .traffy_fondue import traffy_defs


def get_dbt_definitions():
    return Definitions.merge(
        traffy_defs
    )
