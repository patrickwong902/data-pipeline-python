from data_pipeline.base_classes.data_structures.source import SourceConfig
from data_pipeline.base_classes.data_structures.sink import SinkConfig
from data_pipeline.bronze_to_silver.transform.logic.utils import get_schema


class BronzeToSilver:
    def __init__(self, source_container_name, sink_container_name, storage_account):
        clean_nba_source_config = SourceConfig(
            storage_account=storage_account,
            container_name=source_container_name
        )
        clean_nba_sink_config = SinkConfig(
            storage_account=storage_account,
            container_name=sink_container_name,
            mode="overwrite"
        )

    def process_nba(self):
        get_schema("NBA_Regular_Season")






