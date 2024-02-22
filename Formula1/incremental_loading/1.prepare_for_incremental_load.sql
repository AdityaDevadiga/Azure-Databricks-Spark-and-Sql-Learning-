-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Drop all the tables

-- COMMAND ----------

drop database if exists f1_processed cascade

-- COMMAND ----------

create database if not exists f1_processed
location  "/mnt/formula1dladi1/processed"

-- COMMAND ----------

drop database if exists f1_presentation cascade

-- COMMAND ----------

create database if not exists f1_presentation
location "/mnt/formula1dladi1/presentation"

-- COMMAND ----------

