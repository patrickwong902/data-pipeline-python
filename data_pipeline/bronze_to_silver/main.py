from data_pipeline.bronze_to_silver.transform.processor import Processor
from data_pipeline.bronze_to_silver.transform.sink_source_manager import load_data, get_schema, write_data
from data_pipeline.base_classes.data_structures.source import SourceConfig
from data_pipeline.base_classes.data_structures.sink import SinkConfig


def bronze_to_silver(storage_account, source_storage_name, sink_storage_name, spark):
    source_config = SourceConfig(storage_account=storage_account, container_name=source_storage_name)
    sink_config = SinkConfig(storage_account=storage_account,
                             container_name=sink_storage_name, mode="overwrite")

    schema_nba = get_schema("NBA_Regular_Season")

    dataframe_nba = load_data(spark=spark, source_config=source_config,
                              path="NBA_Regular_Season/2002-03 NBA - Sheet1.csv",
                              file_format="csv", schema=schema_nba)
    dataframe_nba_cleaned = Processor(dataframe=dataframe_nba).process_nba()
    write_data(sink_config=sink_config, file_format="delta", dataframe=dataframe_nba_cleaned, path="NBA_Regular_Season")
