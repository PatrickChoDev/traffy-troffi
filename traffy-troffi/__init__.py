from dagster import Definitions
from .dlt.definitions import get_dlt_definitions
from .dbt.definitions import get_dbt_definitions

defs = Definitions.merge(
    get_dlt_definitions(),
    get_dbt_definitions()
)