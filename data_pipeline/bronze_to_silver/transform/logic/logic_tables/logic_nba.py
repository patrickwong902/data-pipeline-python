from data_pipeline.bronze_to_silver.transform.logic.transformation import Transformation
from data_pipeline.bronze_to_silver.transform.logic.logic import Logic


class LogicNBA(Logic):
    def __init__(self, dataframe):
        super().__init__(dataframe)

    def rename_columns(self):
        self.dataframe = Transformation(self.dataframe).rename_column(column_name_old="3P%",
                                                                      column_name_new="3P_Percentage")
        self.dataframe = Transformation(self.dataframe).rename_column(column_name_old="2P%",
                                                                      column_name_new="2P_Percentage")
        self.dataframe = Transformation(self.dataframe).rename_column(column_name_old="FT%",
                                                                      column_name_new="FT_Percentage")
        return self.dataframe
