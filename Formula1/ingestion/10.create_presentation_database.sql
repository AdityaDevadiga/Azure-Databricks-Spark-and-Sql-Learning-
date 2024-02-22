-- Databricks notebook source
create database if not exists f1_presentation
location "/mnt/formula1dladi1/presentation"

-- COMMAND ----------

describe database f1_presentation

-- COMMAND ----------

describe extend
schema f1_presentation

-- COMMAND ----------

