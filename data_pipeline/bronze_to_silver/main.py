from data_pipeline.bronze_to_silver.transform.processor import Processor
from data_pipeline.bronze_to_silver.transform.sink_source_manager import load_data, get_schema, write_data
from data_pipeline.base_classes.data_structures.source import SourceConfig
from data_pipeline.base_classes.data_structures.sink import SinkConfig
from data_pipeline.pipeline_config import config
from datetime import datetime

current_date = datetime.now()
formatted_date = current_date.strftime("%Y.%m.%d")

file_list = config["bronze->silver"]["files"]


def bronze_to_silver(storage_account, source_storage_name, sink_storage_name, spark):
    source_config = SourceConfig(storage_account=storage_account, container_name=source_storage_name)
    sink_config = SinkConfig(storage_account=storage_account,
                             container_name=sink_storage_name, mode="overwrite")

    for file_name in file_list:
        schema = get_schema(file_name)
        dataframe = load_data(spark=spark, source_config=source_config,
                              path=f"{file_name}/{formatted_date}.csv",
                              file_format="csv", schema=schema)
        dataframe_cleaned = Processor(dataframe=dataframe).process_nba()
        write_data(sink_config=sink_config, file_format="delta", dataframe=dataframe_cleaned, path=file_name)

