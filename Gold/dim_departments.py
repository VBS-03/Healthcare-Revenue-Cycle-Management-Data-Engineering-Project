# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare_rcm_catalog.gold.dim_departments
# MAGIC (
# MAGIC Dept_Id string,
# MAGIC SRC_Dept_Id string,
# MAGIC Name string,
# MAGIC datasource string
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate TABLE healthcare_rcm_catalog.gold.dim_departments

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into healthcare_rcm_catalog.gold.dim_departments
# MAGIC select 
# MAGIC distinct
# MAGIC Dept_Id ,
# MAGIC SRC_Dept_Id ,
# MAGIC Name ,
# MAGIC datasource 
# MAGIC  from healthcare_rcm_catalog.silver.departments
# MAGIC  where is_quarantined=false

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from healthcare_rcm_catalog.gold.dim_departments