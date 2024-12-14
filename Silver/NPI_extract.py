# Databricks notebook source
df_npi_extract=spark.read.parquet("/mnt/rcmdatalake03/bronze/npi_extract")
df_npi_extract.createOrReplaceTempView('npi_extract') 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare_rcm_catalog.silver.npi_extract (
# MAGIC   npi_id STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   position STRING,
# MAGIC   organisation_name STRING,
# MAGIC   last_updated STRING,
# MAGIC   inserted_date DATE,
# MAGIC   updated_date DATE,
# MAGIC   is_current_flag BOOLEAN
# MAGIC );   

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO
# MAGIC   healthcare_rcm_catalog.silver.npi_extract AS target
# MAGIC USING
# MAGIC   npi_extract AS source
# MAGIC ON target.npi_id = source.npi_id and target.is_current_flag = true
# MAGIC WHEN MATCHED AND
# MAGIC   target.first_name != source.first_name OR
# MAGIC   target.last_name != source.last_name OR
# MAGIC   target.position != source.position OR
# MAGIC   target.organisation_name != source.organisation_name OR
# MAGIC   target.last_updated != source.last_updated
# MAGIC   THEN UPDATE SET
# MAGIC   target.updated_date = current_date,
# MAGIC   target.is_current_flag = False

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO
# MAGIC   healthcare_rcm_catalog.silver.npi_extract AS target
# MAGIC USING
# MAGIC   npi_extract AS source
# MAGIC ON target.npi_id = source.npi_id and target.is_current_flag = true
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   npi_id,
# MAGIC   first_name ,
# MAGIC   last_name ,
# MAGIC   position ,
# MAGIC   organisation_name ,
# MAGIC   last_updated ,
# MAGIC   inserted_date ,
# MAGIC   updated_date ,
# MAGIC   is_current_flag 
# MAGIC )
# MAGIC   VALUES (
# MAGIC   source.npi_id,
# MAGIC   source.first_name ,
# MAGIC   source.last_name ,
# MAGIC   source.position ,
# MAGIC   source.organisation_name ,
# MAGIC   source.last_updated ,
# MAGIC   current_date,
# MAGIC   current_date, 
# MAGIC   true
# MAGIC   );

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from healthcare_rcm_catalog.silver.npi_extract