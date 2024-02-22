# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using sas token
# MAGIC 1.Set the spark config for sas token
# MAGIC 2.List files from demo container.
# MAGIC 3.read data from circuits.csv

# COMMAND ----------

formula1dladi1_account_key = dbutils.secrets.get(scope = "formula1dladi1-scope",key ='formula1dladi1-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dladi1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dladi1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dladi1.dfs.core.windows.net",formula1dladi1_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dladi1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dladi1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

