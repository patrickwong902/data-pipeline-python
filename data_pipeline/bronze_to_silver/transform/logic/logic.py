from data_pipeline.bronze_to_silver.transform.logic.utils import load_data, write_data, get_schema


class LogicNBA:
    def __init__(self, source_config, sink_config, table_name):
        self.source_config = source_config
        self.sink_config = sink_config
        self.table_name = table_name
        self.schema = None
        self.dataframe = None

    def get_schema(self):
        self.schema = get_schema(self.table_name)

    def load_data(self, spark, path, file_format):
        self.get_schema()
        self.dataframe = load_data(spark=spark, source_config=self.source_config, path=path,
                                   file_format=file_format, schema=self.schema)

    def write_data(self, path):
        write_data(path=path, sink_config=self.sink_config, dataframe=self.dataframe, file_format="delta")