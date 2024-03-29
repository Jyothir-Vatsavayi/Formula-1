# Databricks notebook source
v_result = dbutils.notebook.run("1.inget_circuits_file", 0 , {"p_data_source": "Ergast APT", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0 , {"p_data_source": "Ergast APT", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file", 0 , {"p_data_source": "Ergast APT", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file", 0 , {"p_data_source": "Ergast APT", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file", 0 , {"p_data_source": "Ergast APT", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops_file", 0 , {"p_data_source": "Ergast APT", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_folder", 0 , {"p_data_source": "Ergast APT", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_folder", 0 , {"p_data_source": "Ergast APT", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------


