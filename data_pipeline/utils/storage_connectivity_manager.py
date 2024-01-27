import os


def config_spark_to_access_adls(spark, storage_account_name):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        tenant_id = os.getenv('tenant_id')
        client_id = os.getenv('client_id')
        secret_scope = os.getenv('secret_scope')
        client_secret = dbutils.secrets.get(scope=secret_scope, key=os.getenv('secret_name'))
        spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",
                       "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net",
                       client_id)
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net",
                       client_secret)
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net",
                       f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
    except ImportError:
        print("Not running in Databricks Runtime. Skipping DBUtils configuration")
    return spark
