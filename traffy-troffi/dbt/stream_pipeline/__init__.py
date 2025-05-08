from dagster import Definitions

from .predictor import ml_predictor_defs
from .tagger import tagger_defs
from .create_untagged_table import traffy_fondue_untagged_postgres_table
from .create_unpredicted_table import traffy_fondue_unpredicted_postgres_table

stream_pipeline_defs = Definitions.merge(
    ml_predictor_defs,
    tagger_defs,
    Definitions(
        assets=[traffy_fondue_untagged_postgres_table,
                traffy_fondue_unpredicted_postgres_table]
    )
)
