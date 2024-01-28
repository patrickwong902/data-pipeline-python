import unittest
from data_pipeline.bronze_to_silver.transform.logic.logic import LogicNBA
from data_pipeline.base_classes.data_structures.sink import SinkConfig
from data_pipeline.base_classes.data_structures.source import SourceConfig
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
storage_account = ""
source_container_name = ""
sink_container_name = ""
table_name = ""

source_config = SourceConfig(
    storage_account=storage_account,
    container_name=source_container_name
)
sink_config = SinkConfig(
    storage_account=storage_account,
    container_name=sink_container_name,
    mode="overwrite"
)


class TestLogicNBA(unittest.TestCase):
    def setUp(self) -> None:
        table_data = [
            {"3P%": 0.5,
             "2P%": 0.4,
             "FT%": 0.8}
        ]
        self.dataframe = spark.createDataFrame(table_data)

    def test_rename_column(self):
        logic_nba = LogicNBA(source_config=source_config, sink_config=sink_config, table_name=table_name)
        logic_nba.dataframe = self.dataframe
        logic_nba.rename_columns()
        dataframe = logic_nba.dataframe
        assert dataframe.columns[0] == "2P_Percentage"
        assert dataframe.columns[1] == "3P_Percentage"
        assert dataframe.columns[2] == "FT_Percentage"


if __name__ == '__main__':
    unittest.main()
