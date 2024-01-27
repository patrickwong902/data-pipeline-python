from data_pipeline.base_classes.local_source_base import LocalSourceBase
from data_pipeline.base_classes.data_structures.source import SourceConfig


class SourceLocalStrategy(LocalSourceBase):

    def __init__(self, dataframe, source_config: SourceConfig):
        super().__init__(dataframe=dataframe, source_config=source_config)