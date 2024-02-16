# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capailities of the dbutils.secret utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope', key = 'Formula1dlsg-account-key')

# COMMAND ----------


