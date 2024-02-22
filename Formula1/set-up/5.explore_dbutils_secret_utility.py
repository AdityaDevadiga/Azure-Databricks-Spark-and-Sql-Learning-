# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1dladi1-scope', key = 'formula1dladi1-account-key')

# COMMAND ----------

