class Transformation:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def rename_column(self, column_name_old, column_name_new):
        self.dataframe = self.dataframe.withColumnRenamed(column_name_old, column_name_new)
        return self.dataframe
