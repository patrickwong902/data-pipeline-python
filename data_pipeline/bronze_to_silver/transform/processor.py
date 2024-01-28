from data_pipeline.bronze_to_silver.transform.logic.logic_tables.logic_nba import LogicNBA
from data_pipeline.bronze_to_silver.transform.logic.logic_tables.logic_playoff import LogicPlayoff


class Processor:
    def __init__(self, dataframe, file_name):
        self.dataframe = dataframe
        self.file_name = file_name

    def processor(self):
        if self.file_name == "NBA_Regular_Season":
            logic_object = LogicNBA(dataframe=self.dataframe)
            return logic_object.rename_columns()

        else:
            logic_object = LogicPlayoff(dataframe=self.dataframe)
            return logic_object.aggregate_team_points()








