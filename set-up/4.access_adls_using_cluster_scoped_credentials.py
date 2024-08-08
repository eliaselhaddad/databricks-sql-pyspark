# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azur.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@eliasf1deltalake.dfs.core.windows.net"))

# COMMAND ----------

# use the spark api to read the data
display(spark.read.csv("abfss://demo@eliasf1deltalake.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


