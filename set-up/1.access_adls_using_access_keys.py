# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azur.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-account-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.eliasf1deltalake.dfs.core.windows.net", formula1dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@eliasf1deltalake.dfs.core.windows.net"))

# COMMAND ----------

# use the spark api to read the data
display(spark.read.csv("abfss://demo@eliasf1deltalake.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


