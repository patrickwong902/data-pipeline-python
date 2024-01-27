from pyspark.sql.types import IntegerType, DoubleType, StringType


schema_dictionary = {
    'integer': IntegerType(),
    'double': DoubleType(),
    'string': StringType()
}