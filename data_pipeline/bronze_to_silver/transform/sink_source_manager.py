from data_pipeline.bronze_to_silver.source.source_local_strategy import SourceLocalStrategy
from data_pipeline.bronze_to_silver.sink.sink_local_strategy import SinkLocalStrategy
from data_pipeline.bronze_to_silver.transform.schema.generate_schema import GenerateSourceSchema
from data_pipeline.pipeline_config import config

strategy_map = {
    "source": {
        "local": SourceLocalStrategy
    },
    "sink": {
        "local": SinkLocalStrategy
    }
}
strategy = config["bronze->silver"]["strategy"]


def load_data(spark, source_config, path, file_format, schema):
    source_strategy = strategy_map["source"][strategy["source"]](source_config=source_config)
    return source_strategy.read(spark=spark, path=path, file_format=file_format, schema=schema)


def get_schema(table_name):
    return GenerateSourceSchema(table_name=table_name).generate_schema()


def write_data(path, sink_config, file_format, dataframe):
    sink_strategy = strategy_map["sink"][strategy["sink"]](dataframe=dataframe, sink_config=sink_config)
    return sink_strategy.write(path=path, file_format=file_format)
