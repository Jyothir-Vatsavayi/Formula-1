# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using service principle
# MAGIC ###### Steps to Follow
# MAGIC 1. Register Azure AS Application/Service Principle
# MAGIC 1. Generate secret/password for Application
# MAGIC 1. Set spark config with App/Client Id, Directory/ Tenant Id & Secret
# MAGIC 1. Assign Role "Storage Blob Data Contriuter" to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope= 'formula1-scope', key = 'formula1-client-id')
tenant_id = dbutils.secrets.get(scope= 'formula1-scope', key = 'formula1-tenent-id')
client_secret = dbutils.secrets.get(scope= 'formula1-scope', key = 'formula1-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlsg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlsg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlsg.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlsg.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlsg.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlsg.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlsg.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


