from data_pipeline.bronze_to_silver.transform.transformation_manager import TransformationManager


def apply_business_logic(storage_account, source_container_name, sink_container_name, spark):
    TransformationManager(source_container_name=source_container_name,
                          sink_container_name=sink_container_name,
                          storage_account=storage_account).process_nba(spark)
