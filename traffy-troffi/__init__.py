from dagster import Definitions

from .dbt.definitions import get_dbt_definitions
from .dlt.definitions import get_dlt_definitions

defs = Definitions.merge(
    get_dlt_definitions(),
    get_dbt_definitions()
)
