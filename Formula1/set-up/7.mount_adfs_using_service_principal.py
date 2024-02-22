# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC 1.Get the client_id,tenant_id and cilent_secrets from key_vault
# MAGIC 2.Set Spark Config with App/ Cient id,Directory/Tenant id & Secret
# MAGIC 3.Call file system utility mount to mount the storage.
# MAGIC 4.Explore other file system utilities related to mount(list all mounts,unmounts)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1dladi1-scope', key = 'formula1dladi1-app-clinet-id')
tenant_id = dbutils.secrets.get(scope = 'formula1dladi1-scope', key = 'formula1dladi1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1dladi1-scope', key = 'formula1dladi1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
    source="abfss://demo@formula1dladi1.dfs.core.windows.net",
    mount_point="/mnt/formula1dladi1/demo",
    extra_configs=configs
)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dladi1/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dladi1/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

