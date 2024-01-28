from pyspark.sql.functions import sum


class Transformation:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def rename_column(self, column_name_old, column_name_new):
        self.dataframe = self.dataframe.withColumnRenamed(column_name_old, column_name_new)
        return self.dataframe

    def aggregate(self, group_by_columns, key, key_alias):
        self.dataframe = self.dataframe.groupBy(*group_by_columns) \
            .agg(sum(key).alias(key_alias))
        return self.dataframe

    def joining_nba(self, dataframe_nba):
        self.dataframe = dataframe_nba.join(self.dataframe, on="Tm", how="right")
        return self.dataframe
