# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare_rcm_catalog.gold.dim_npi (
# MAGIC   npi_id STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   position STRING,
# MAGIC   organisation_name STRING,
# MAGIC   last_updated STRING,
# MAGIC   refreshed_at TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC truncate table healthcare_rcm_catalog.gold.dim_npi

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC insert into
# MAGIC   healthcare_rcm_catalog.gold.dim_npi
# MAGIC select
# MAGIC   npi_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   position,
# MAGIC   organisation_name,
# MAGIC   last_updated,
# MAGIC   current_timestamp() as refreshed_at
# MAGIC from
# MAGIC   healthcare_rcm_catalog.silver.npi_extract
# MAGIC   where is_current_flag = true