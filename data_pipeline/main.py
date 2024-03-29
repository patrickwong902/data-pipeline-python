from data_pipeline.utils.spark_session_manager import get_spark_session
from data_pipeline.bronze_to_silver.main import bronze_to_silver
from data_pipeline.utils.storage_connectivity_manager import config_spark_to_access_adls

spark = get_spark_session()
ENV = ""
storage_account = ""
if ENV == "DEV" or ENV == "TEST" or ENV == "PROD":
    storage_account = f"{ENV}storage"
else:
    from local_config import LOCAL_DATA_ROOT
    storage_account = LOCAL_DATA_ROOT

raw_container_name = "bronze"
cleansed_container_name = "silver"
curated_container_name = "gold"
export_container_name = "export"


spark = config_spark_to_access_adls(spark=spark, storage_account_name=storage_account)


def main():
    bronze_to_silver(spark=spark, storage_account=storage_account, source_storage_name=raw_container_name,
                     sink_storage_name=cleansed_container_name)


if __name__ == '__main__':
    main()
