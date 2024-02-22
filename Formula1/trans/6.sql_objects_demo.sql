-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### LEARNING OBJECTIVES
-- MAGIC 1.Spqrk Sql documentation
-- MAGIC 2.Create Database demo
-- MAGIC 3.Data tab in the UI
-- MAGIC 4.SHOW command
-- MAGIC 5.DESCRIBE command
-- MAGIC 6.Find the current database

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC                                      

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

select * from demo.race_results_python where race_year = 2020

-- COMMAND ----------

create table demo.race_results_sql
as
select * from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

select current_database()

-- COMMAND ----------

desc extended demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC 1.Create external table using Python
-- MAGIC 2.Create external table using SQL
-- MAGIC 3.Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

describe extended demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
using parquet
Location "/mnt/formula1dladi1/presentation/race_results_ext_sql"

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_py where race_year = 2020

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql

-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### VIEWS ON TABLES
-- MAGIC LEARNING OBJECTIVES
-- MAGIC 1.Create Temp View
-- MAGIC 2.Create Global Temp view
-- MAGIC 3.Create permanet View

-- COMMAND ----------

create temp view v_race_results
as
select *
from race_results_python
where race_year = 2020

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create or replace global temp view gv_race_results
as
select *
from race_results_python
where race_year = 2020

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

create or replace  view demo.pv_race_results
as
select *
from race_results_python
where race_year = 2020

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

