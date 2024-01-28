from data_pipeline.bronze_to_silver.transform.logic.transformation import Transformation
from data_pipeline.bronze_to_silver.transform.logic.logic import Logic


class LogicPlayoff(Logic):
    def __init__(self, dataframe):
        super().__init__(dataframe)

    def aggregate_team_points(self):
        self.dataframe = Transformation(self.dataframe).aggregate(["Tm"], "PTS", "PTS_Total")
        return self.dataframe

