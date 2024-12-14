# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating mounts to ADLS containers using Service Principal

# COMMAND ----------

storage_account_name = "rcmdatalake03"
client_id            = dbutils.secrets.get(scope="healthcare-rcm-secret-scope", key="healthcare-rcm-adb-app-client-id")
tenant_id            = dbutils.secrets.get(scope="healthcare-rcm-secret-scope", key="healthcare-rcm-adb-app-tenant-id")
client_secret        = dbutils.secrets.get(scope="healthcare-rcm-secret-scope", key="healthcare-rcm-adb-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

mountPoints=["gold","silver","bronze","landing","configs"]
for mountPoint in mountPoints:
    if not any(mount.mountPoint == f"/mnt/{storage_account_name}/{mountPoint}" for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
            source = f"abfss://{mountPoint}@{storage_account_name}.dfs.core.windows.net/",
            mount_point = f"/mnt/{storage_account_name}/{mountPoint}",
            extra_configs = configs
            )
            print(f"{mountPoint} mount succeeded!")
        except Exception as e:
            print("mount exception", e)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/rcmdatalake03/"))