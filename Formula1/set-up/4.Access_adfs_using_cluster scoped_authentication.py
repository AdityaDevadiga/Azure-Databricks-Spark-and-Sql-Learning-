# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using access keys
# MAGIC 1.Set the spark config fs.azure.account.key in the cluster
# MAGIC 2.List files from demo container.
# MAGIC 3.read data from circuits.csv

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dladi1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dladi1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

