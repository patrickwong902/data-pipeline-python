import unittest
from pyspark.sql import SparkSession
from data_pipeline.bronze_to_silver.transform.logic.transformation import Transformation

spark = SparkSession.builder.appName("test").getOrCreate()


class TestTransformation(unittest.TestCase):
    def setUp(self) -> None:
        table_data = [
            {"3P%": 0.5,
             "2P%": 0.4,
             "FT%": 0.8}
        ]
        self.dataframe = spark.createDataFrame(table_data)

        table_data_non_agg = [
            {"Tm": "Team1",
             "PTS": 30},
            {"Tm": "Team2",
             "PTS": 25},
            {"Tm": "Team1",
             "PTS": 30}

        ]
        self.dataframe_non_agg = spark.createDataFrame(table_data_non_agg)

    def test_rename_column(self):
        clean_dataframe = Transformation(self.dataframe)
        clean_dataframe.rename_column("2P%", "2P_Percentage")
        dataframe_cleaned = clean_dataframe.dataframe
        assert dataframe_cleaned.columns[0] == "2P_Percentage"

    def test_aggregation(self):
        agg_dataframe = Transformation(self.dataframe_non_agg)
        dataframe = agg_dataframe.aggregate(group_by_columns=["Tm"], key="PTS", key_alias="PTS_Total")
        print(dataframe.columns[0])
        assert True


if __name__ == '__main__':
    unittest.main()
