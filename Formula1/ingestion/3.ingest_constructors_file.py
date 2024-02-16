# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

contructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
.schema(contructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Dropping unwanted columns from dataframe

# COMMAND ----------

# MAGIC %md 
# MAGIC 1. we can drop the column in various ways
# MAGIC 1. If there is only one dataframe using the below method was best approch
# MAGIC 1. constructors_dropped_df = constructors_df.drop(col('url'))
# MAGIC 1. If we has more than 1 dataframes we need to specify the dataframe when dropping the column
# MAGIC 1. constructors_dropped_df = constructors_df.drop(col('url'))
# MAGIC 1. for using the above method we need to import the col first.
# MAGIC 1. from pyspark.sql.functions import col
# MAGIC

# COMMAND ----------

# constructors_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(constructors_df['url'])

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 - Rename columns and add the ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
                                                .withColumnRenamed("constructorRef", "constructor_ref")\
                                                .withColumn("ingestion_date", current_timestamp())\
                                                .withColumn("data_source", lit(v_data_source))\
                                                .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed_constructors

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


