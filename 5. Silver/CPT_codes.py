# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

#Reading Hospital A departments data 
df_cpt_codes = spark.read.parquet("/mnt/rcmdatalake03/bronze/cpt_codes/*.parquet")

display(df_cpt_codes)

df_cpt_codes.createOrReplaceTempView("cpt_codes")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC  cpt_codes,
# MAGIC  procedure_code_category,
# MAGIC  procedure_code_descriptions,
# MAGIC  code_status,
# MAGIC     CASE 
# MAGIC         WHEN cpt_codes IS NULL OR procedure_code_descriptions IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM cpt_codes

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from quality_checks

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare_rcm_catalog.silver.cpt_codes (
# MAGIC cpt_codes string,
# MAGIC procedure_code_category string,
# MAGIC procedure_code_descriptions string,
# MAGIC code_status string,
# MAGIC is_quarantined boolean,
# MAGIC audit_insertdate timestamp,
# MAGIC audit_modifieddate timestamp,
# MAGIC is_current boolean
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update old record to implement SCD Type 2
# MAGIC MERGE INTO healthcare_rcm_catalog.silver.cpt_codes AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.cpt_codes = source.cpt_codes AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.procedure_code_category != source.procedure_code_category OR
# MAGIC     target.procedure_code_descriptions != source.procedure_code_descriptions OR
# MAGIC     target.code_status != source.code_status OR
# MAGIC     target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC   UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.audit_modifieddate = current_timestamp()
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new record to implement SCD Type 2
# MAGIC MERGE INTO healthcare_rcm_catalog.silver.cpt_codes AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.cpt_codes = source.cpt_codes AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     cpt_codes,
# MAGIC     procedure_code_category,
# MAGIC     procedure_code_descriptions,
# MAGIC     code_status,
# MAGIC     is_quarantined,
# MAGIC     audit_insertdate,
# MAGIC     audit_modifieddate,
# MAGIC     is_current
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.cpt_codes,
# MAGIC     source.procedure_code_category,
# MAGIC     source.procedure_code_descriptions,
# MAGIC     source.code_status,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     true
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  healthcare_rcm_catalog.silver.cpt_codes