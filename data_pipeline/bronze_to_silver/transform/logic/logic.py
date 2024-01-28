from data_pipeline.bronze_to_silver.transform.logic.transformation import Transformation


class LogicNBA:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def rename_columns(self):
        self.dataframe = Transformation(self.dataframe).rename_column(column_name_old="3P%",
                                                                      column_name_new="3P_Percentage")
        self.dataframe = Transformation(self.dataframe).rename_column(column_name_old="2P%",
                                                                      column_name_new="2P_Percentage")
        self.dataframe = Transformation(self.dataframe).rename_column(column_name_old="FT%",
                                                                      column_name_new="FT_Percentage")
        return self.dataframe
