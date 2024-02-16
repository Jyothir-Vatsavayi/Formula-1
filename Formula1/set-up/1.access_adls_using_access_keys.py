# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List file from demo container
# MAGIC 1. Read data from circuits.csv file.

# COMMAND ----------

formula1_account_key = dbutils.secrets.get(scope='formula1-scope', key = 'Formula1dlsg-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlsg.dfs.core.windows.net",
    formula1_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlsg.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlsg.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


