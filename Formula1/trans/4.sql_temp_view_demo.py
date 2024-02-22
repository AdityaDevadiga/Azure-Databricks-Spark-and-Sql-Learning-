# Databricks notebook source
# MAGIC %md
# MAGIC ### ACCESS dataframes using the SQL
# MAGIC Objectives
# MAGIC 1.Create temp views on dtaframe.
# MAGIC 2.Acces the views from SQL cell
# MAGIC 3.Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) from v_race_results where race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM v_race_results where race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###GLOBAL Temp

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from global_temp.gv_race_results

# COMMAND ----------

