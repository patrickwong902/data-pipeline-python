from data_pipeline.base_classes.data_structures.source import SourceConfig


class LocalSourceBase:
    def __init__(self, source_config: SourceConfig):
        self.storage_account = source_config.storage_account
        self.container_name = source_config.container_name
        self.dataframe = None
        self.full_path = None

    def create_full_path(self, path):
        self.full_path = "/".join([
            self.storage_account,
            self.container_name,
            path
        ])

    def read(self, spark, path, file_format, schema=None):
        print("Reading from (local) Data Lake started")
        self.create_full_path(path)
        print(f"Reading from {self.full_path}")
        if file_format == "csv":
            self.dataframe = spark.read.option("header", True).option("delimiter", ",").format(
                file_format) \
                .load(path=self.full_path, schema=schema)
        else:
            self.dataframe = spark.read.format(file_format).load(path=self.full_path, schema=schema)
        print(f"Reading from (local) Data Lake is done")
        return self.dataframe
