from pyspark.sql import SparkSession


def get_spark_session(app_name="spark_job"):
    return SparkSession.builder.appName(app_name) \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
