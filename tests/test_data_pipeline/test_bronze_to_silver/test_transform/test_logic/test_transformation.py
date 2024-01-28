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

    def test_rename_column(self):
        clean_dataframe = Transformation(self.dataframe)
        clean_dataframe.rename_column("2P%", "2P_Percentage")
        dataframe_cleaned = clean_dataframe.dataframe
        assert dataframe_cleaned.columns[0] == "2P_Percentage"


if __name__ == '__main__':
    unittest.main()
