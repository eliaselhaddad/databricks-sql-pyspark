# Databricks notebook source
# MAGIC %md
# MAGIC - Write data to delta lake - managed tables
# MAGIC - Write data to delta lake - external tables
# MAGIC - Read data from delta lake - table
# MAGIC - Read data from delta lake - file

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/eliasf1deltalake/demo'

# COMMAND ----------

results_df = spark.read.option('inferSchema', True).json('/mnt/eliasf1deltalake/raw/2021-03-28/results.json')

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable('f1_demo.results_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save('/mnt/eliasf1deltalake/demo/results_external')

# COMMAND ----------

# %sql
# CREATE TABLE f1_demo.results_external
#  USING DELTA
#  LOCATION '/mnt/eliasf1deltalake/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external;

# COMMAND ----------

results_external_df = spark.read.format("delta").load('/mnt/eliasf1deltalake/demo/results_external')

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable('f1_demo.results_partitioned')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC - Update Delta table
# MAGIC - Delete from Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed 
# MAGIC SET points = 11 - position
# MAGIC WHERE position < 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/mnt/eliasf1deltalake/demo/results_managed")

delta_table.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using merge

# COMMAND ----------



# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
