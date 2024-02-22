-- Databricks notebook source
show databases

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from drivers limit 100

-- COMMAND ----------

desc drivers

-- COMMAND ----------

select name,dob from drivers where nationality = "British" and dob >= '1990-01-01' order by dob desc

-- COMMAND ----------





select name,dob,nationality from drivers where (nationality = "British" and dob >= '1990-01-01') or nationality = "Indian" order by dob desc

-- COMMAND ----------

