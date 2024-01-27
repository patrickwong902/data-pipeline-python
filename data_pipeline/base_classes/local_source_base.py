from abc import ABC
from data_pipeline.base_classes.data_structures.source import SourceConfig


class LocalSourceBase(ABC):
    def __init__(self, source_config: SourceConfig):
        super().__init__()
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
        self.dataframe = spark.read.format(file_format).load(self.full_path, schema=schema)
        print(f"Reading from (local) Data Lake is done")

