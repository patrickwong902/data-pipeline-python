from data_pipeline.bronze_to_silver.transform.logic.logic_tables.logic_nba import LogicNBA


class Processor:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def process_nba(self):
        logic_nba_object = LogicNBA(dataframe=self.dataframe)
        return logic_nba_object.rename_columns()








