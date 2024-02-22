-- Databricks notebook source
create database if not exists f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  CREATE A TABLE

-- COMMAND ----------

drop table if exists f1_raw.circuits;

CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
using csv 
options (path "/mnt/formula1dladi1/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### RACES TABLES

-- COMMAND ----------

drop table if exists f1_raw.races;

create table if not exists f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
using csv
options (path "/mnt/formula1dladi1/raw/races.csv",header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE pit stops table
-- MAGIC MULTI LINE JSON

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
using json
options(path "/mnt/formula1dladi1/raw/pit_stops.json" ,multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE THE TABLES FOR OF LISt OF FILES

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
using csv
options (path "/mnt/formula1dladi1/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### QUALIFYING THE TABLES

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT
)
using json
options (path "/mnt/formula1dladi1/raw/qualifying",multiLine true)


-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

.