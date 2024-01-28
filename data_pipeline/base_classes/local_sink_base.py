from data_pipeline.base_classes.data_structures.sink import SinkConfig


class LocalSinkBase:
    def __init__(self, dataframe, sink_config: SinkConfig):
        self.mode = sink_config.mode
        self.storage_account = sink_config.storage_account
        self.container_name = sink_config.container_name
        self.dataframe = dataframe
        self.full_path = None

    def create_full_path(self, path):
        self.full_path = "/".join([
            self.storage_account,
            self.container_name,
            path
        ])

    def write(self, path, file_format):
        print("Writing to (local) Data Lake started")
        self.create_full_path(path)
        print(f"Writing to {self.full_path}")
        self.dataframe.write.format(file_format).option(
            "overWriteSchema", "true").mode(self.mode).save(path=self.full_path)
        print("Writing to (local) Data Lake is done!")
