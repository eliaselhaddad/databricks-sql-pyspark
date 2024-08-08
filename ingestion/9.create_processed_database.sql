-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION '/mnt/eliasf1deltalake/processed'

-- COMMAND ----------

DESC DATABASE f1_processed;
