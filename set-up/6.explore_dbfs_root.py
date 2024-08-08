# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore DBFS root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 2. Interact with DBFS file browser
# MAGIC 3. Upload files to DBFS root

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore"))

# COMMAND ----------

circuits_df = spark.read.csv("/FileStore/circuits.csv", header=True, inferSchema=True)
display(circuits_df)
