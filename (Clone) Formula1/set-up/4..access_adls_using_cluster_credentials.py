# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark conf in cluster Advanced setting (spark config)
# MAGIC 1. List file from demo container
# MAGIC 1. Read data from circuits.csv file.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Instead of writing below spark config here we need to update this config in respectve cluster itself
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.key.formula1dlsg.dfs.core.windows.net",
# MAGIC     "7PKVklWWZsoCfhxBRnwbijQ1VtvOpSmYgEYiqytjrUfsGPrRzrESGodje9Hz34daFRpeMbNJFuKt+ASt9vfT1g=="
# MAGIC )

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlsg.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlsg.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


