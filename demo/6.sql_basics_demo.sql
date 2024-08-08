-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * 
FROM f1_processed.drivers;

-- COMMAND ----------

DESC f1_processed.drivers;

-- COMMAND ----------

SELECT *
 FROM f1_processed.drivers
 WHERE nationality = 'British'
 AND dob >= '1990-01-01';

-- COMMAND ----------

SELECT name, dob AS date_of_birth
 FROM f1_processed.drivers
 WHERE nationality = 'British'
 AND dob >= '1990-01-01';

-- COMMAND ----------

SELECT name, dob 
 FROM f1_processed.drivers
 WHERE nationality = 'British'
 AND dob >= '1990-01-01'
 ORDER BY dob;

-- COMMAND ----------

SELECT * 
 FROM f1_processed.drivers
 ORDER BY nationality, dob DESC;

-- COMMAND ----------

SELECT name, nationality, dob 
 FROM f1_processed.drivers
 WHERE (nationality = 'British'
 AND dob >= '1990-01-01')
 OR (nationality = 'Indian')
 ORDER BY dob DESC;

-- COMMAND ----------


