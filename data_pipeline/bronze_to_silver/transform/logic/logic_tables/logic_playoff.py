from data_pipeline.bronze_to_silver.transform.logic.transformation import Transformation
from data_pipeline.bronze_to_silver.transform.logic.logic import Logic
from data_pipeline.bronze_to_silver.transform.sink_source_manager import load_data, get_schema


# for some tables, the logic might involve the joining with other tables, that's why source_config is called here
class LogicPlayoff(Logic):
    def __init__(self, dataframe, source_config):
        super().__init__(dataframe)
        self.source_config = source_config

    def playoff_logic(self, spark):
        self.dataframe = Transformation(self.dataframe).aggregate(["Tm"], "PTS", "PTS_Total")
        schema_nba = get_schema(table_name="NBA_Regular_Season")
        dataframe_nba = load_data(spark=spark, path="NBA_Regular_Season/2024.01.28.csv", file_format="csv",
                                  schema=schema_nba, source_config=self.source_config)
        self.dataframe = Transformation(self.dataframe).joining_nba(dataframe_nba=dataframe_nba)
        return self.dataframe

