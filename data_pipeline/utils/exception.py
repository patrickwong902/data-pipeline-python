class NotRunningJobInDatabricksError(Exception):
    print(
        "The job is not running in Databricks. It runs locally"
        " or in the CI pipeline. New Spark session will be created."
    )