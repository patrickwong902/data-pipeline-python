from data_pipeline.base_classes.local_sink_base import LocalStrategyBase
from data_pipeline.base_classes.data_structures.sink import SinkConfig


class SinkLocalStrategy(LocalStrategyBase):

    def __init__(self, dataframe, sink_config: SinkConfig):
        super().__init__(dataframe=dataframe, sink_config=sink_config)
