from data_pipeline.base_classes.data_structures.source import SourceConfig
from data_pipeline.base_classes.data_structures.sink import SinkConfig
from data_pipeline.bronze_to_silver.transform.logic.logic import LogicNBA


class TransformationManager:
    def __init__(self, source_container_name, sink_container_name, storage_account):
        source_config = SourceConfig(
            storage_account=storage_account,
            container_name=source_container_name
        )
        sink_config = SinkConfig(
            storage_account=storage_account,
            container_name=sink_container_name,
            mode="overwrite"
        )

        self.logic_nba_object = LogicNBA(source_config, sink_config, table_name="NBA_Regular_Season")

    def process_nba(self, spark):
        self.logic_nba_object.load_data(spark, path="NBA_Regular_Season/2002-03 NBA - Sheet1.csv", file_format="csv")
        self.logic_nba_object.rename_columns()
        self.logic_nba_object.write_data(path="NBA_Regular_Season")






