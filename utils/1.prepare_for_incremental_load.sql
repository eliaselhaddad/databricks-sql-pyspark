-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Drop all the tables

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC delta_lake_name = delta_lake_name
-- MAGIC database_location = f"/mnt/{delta_lake_name}/processed"
-- MAGIC
-- MAGIC sql_query = f"""
-- MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
-- MAGIC LOCATION '{delta_lake_name}'
-- MAGIC """
-- MAGIC
-- MAGIC spark.sql(sql_query)

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC delta_lake_name = delta_lake_name
-- MAGIC database_location = f"/mnt/{delta_lake_name}/presentation"
-- MAGIC
-- MAGIC sql_query = f"""
-- MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation
-- MAGIC LOCATION '{delta_lake_name}'
-- MAGIC """
-- MAGIC
-- MAGIC spark.sql(sql_query)
