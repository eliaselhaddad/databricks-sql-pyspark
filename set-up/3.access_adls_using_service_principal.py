# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC ### Steps to follow
# MAGIC 1. Register Azure AD Application/ Service Principal
# MAGIC 1. Generate a secret/ password for the Application
# MAGIC 1. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 1. Assign Role  "Storage Blob Data Contributor" to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-client-id")
tenant_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-client-secret")
storage_account = "eliasf1deltalake"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@eliasf1deltalake.dfs.core.windows.net"))

# COMMAND ----------

# use the spark api to read the data
display(spark.read.csv("abfss://demo@eliasf1deltalake.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


