from pyspark.sql.types import StructField, StructType
from data_pipeline.bronze_to_silver.transform.schema.tables import tables
from data_pipeline.spark_schema_dictionary import schema_dictionary


class GenerateSourceSchema:
    def __init__(self, table_name):
        self.table = tables[table_name]

    def generate_schema(self):
        struct_fields = []
        for column in self.table:
            struct_fields.append(StructField(column, schema_dictionary[column], True))
        return StructType(struct_fields)


