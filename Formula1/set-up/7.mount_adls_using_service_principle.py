# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using service principle
# MAGIC ###### Steps to Follow
# MAGIC 1. Get client_Id, tenent_Id and client_secret from key vault
# MAGIC 1. Set spark config with App/Client Id, Directory/ Tenant Id & Secret
# MAGIC 1. Call file system utility mount the storage
# MAGIC 1. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope= 'formula1-scope', key = 'formula1-client-id')
tenant_id = dbutils.secrets.get(scope= 'formula1-scope', key = 'formula1-tenent-id')
client_secret = dbutils.secrets.get(scope= 'formula1-scope', key = 'formula1-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlsg.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlsg/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlsg/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlsg/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.mount('/mnt/formula1dlsg/demo')

# COMMAND ----------


