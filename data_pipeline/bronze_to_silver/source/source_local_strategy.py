from data_pipeline.base_classes.local_source_base import LocalSourceBase
from data_pipeline.base_classes.data_structures.source import SourceConfig


class SourceLocalStrategy(LocalSourceBase):

    def __init__(self, source_config: SourceConfig):
        super().__init__(source_config=source_config)