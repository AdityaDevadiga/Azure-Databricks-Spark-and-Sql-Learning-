# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'formula1dladi1-scope', key = 'formula1dladi1-app-clinet-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1dladi1-scope', key = 'formula1dladi1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1dladi1-scope', key = 'formula1dladi1-app-client-secret')

    # Set spark Configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # mount the storage account continer
    dbutils.fs.mount(
        source= f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point= f"/mnt/{storage_account_name}/{container_name}",
        extra_configs=configs)
    
    display(dbutils.fs.mounts())




# COMMAND ----------

mount_adls('formula1dladi1','raw')

# COMMAND ----------

mount_adls('formula1dladi1','processed')

# COMMAND ----------

mount_adls('formula1dladi1','demo')

# COMMAND ----------

mount_adls('formula1dladi1','presentation')

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

dbutils.fs.ls("/mnt/formula1dladi1/demo")

# COMMAND ----------

