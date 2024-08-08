-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
     circuitId INT,
     circuitRef STRING,
     name STRING,
     location STRING,
     country STRING,
     lat DOUBLE,
     lng DOUBLE,
     alt INT,
     url STRING
)
 USING csv
 OPTIONS (path '/mnt/eliasf1deltalake/raw/circuits.csv', header TRUE)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
     raceId INT,
     year INT,
     round INT,
     circuitId INT,
     name STRING,
     date DATE,
     time STRING,
     url STRING
)
 USING csv
 OPTIONS (path '/mnt/eliasf1deltalake/raw/races.csv', header TRUE)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create constructors table
-- MAGIC - single line JSON
-- MAGIC - simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INt, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING,
  url STRING
)
 USING json
 OPTIONS (path '/mnt/eliasf1deltalake/raw/constructors.json')

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table
-- MAGIC - single line JSON
-- MAGIC - complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

CREATE TABLE IF NOT EXISTS f1_raw.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path '/mnt/eliasf1deltalake/raw/drivers.json');

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create results table
-- MAGIC - single line JSON
-- MAGIC - simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

CREATE TABLE IF NOT EXISTS f1_raw.results (
    resultId INT,
    raceId INT,
    driverId INT,
    constructorId INT,
    number INT,
    grid INT,
    position INT,
    positionText STRING,
    positionOrder INT,
    points FLOAT,
    laps INT,
    time STRING,
    milliseconds INT,
    fastestLap INT,
    rank INT,
    fastestLapTime STRING,
    fastestLapSpeed FLOAT,
    statusId STRING
)
USING json
OPTIONS (path '/mnt/eliasf1deltalake/raw/results.json');

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create pit stops table
-- MAGIC - multi line JSON
-- MAGIC - simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
    raceId INT NOT NULL,
    driverId INT,
    stop STRING,
    lap INT,
    time STRING,
    duration STRING,
    milliseconds INT
)
USING json
OPTIONS (
    path '/mnt/eliasf1deltalake/raw/pit_stops.json',
    multiline TRUE
);

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Tables for List of Files
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create lap times table
-- MAGIC - CSV file
-- MAGIC - multiple files 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
    raceId INT,
    driverId INT,
    lap INT,
    position INT,
    time STRING,
    milliseconds INT
)
USING csv
OPTIONS (
    path '/mnt/eliasf1deltalake/raw/lap_times'
);


-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create qualifying table
-- MAGIC - JSON file
-- MAGIC - multiline JSON
-- MAGIC - multiple files 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
    qualifyId INT,
    raceId INT,
    driverId INT,
    constructorId INT,
    number INT,
    position INT,
    q1 STRING,
    q2 STRING,
    q3 STRING
)
USING json
OPTIONS (
    path '/mnt/eliasf1deltalake/raw/qualifying',
    multiline TRUE
);


-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

DESCRIBE EXTENDED f1_raw.qualifying;

-- COMMAND ----------


