# Databricks notebook source
# MAGIC %md
# MAGIC ### Steps to follow
# MAGIC 1. Get client_id, tentant_id and client secret from the key vault
# MAGIC 1. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 1. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-client-id") 
tenant_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-tenant-id") 
client_secret = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-client-secret") 
storage_account = "eliasf1deltalake"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://demo@{storage_account}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account}/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storage_account}/demo"))

# COMMAND ----------

# use the spark api to read the data
display(spark.read.csv(f"/mnt/{storage_account}/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# unmount the storage
# dbutils.fs.unmount(f"/mnt/{storage_account}/demo")

# COMMAND ----------


