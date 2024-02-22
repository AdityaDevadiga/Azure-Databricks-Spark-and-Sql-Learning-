# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC 1.Register Azure AD Application/ Service Principal
# MAGIC 2.Generate a secret/password for the Application.
# MAGIC 3.Set Spark Config with App/ Cient id,Directory/Tenant id & Secret
# MAGIC 4.Assign Role Storage Blob Data Contributor"to Data lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1dladi1-scope',key = 'formula1dladi1-app-clinet-id')
tenant_id = dbutils.secrets.get(scope='formula1dladi1-scope',key = 'formula1dladi1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1dladi1-scope',key = 'formula1dladi1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dladi1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dladi1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dladi1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dladi1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dladi1.dfs.core.windows.net",f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dladi1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dladi1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

