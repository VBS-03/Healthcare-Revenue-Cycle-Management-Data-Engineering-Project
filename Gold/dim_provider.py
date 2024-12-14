# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare_rcm_catalog.gold.dim_provider
# MAGIC (
# MAGIC  ProviderID string,
# MAGIC  FirstName string,
# MAGIC  LastName string,
# MAGIC  DeptID string,
# MAGIC  NPI long,
# MAGIC  datasource string
# MAGIC )

# COMMAND ----------

# MAGIC  %sql 
# MAGIC  truncate TABLE healthcare_rcm_catalog.gold.dim_provider 

# COMMAND ----------

# MAGIC  %sql
# MAGIC insert into healthcare_rcm_catalog.gold.dim_provider
# MAGIC select 
# MAGIC  ProviderID ,
# MAGIC  FirstName ,
# MAGIC  LastName ,
# MAGIC  concat(DeptID,'-',datasource) deptid,
# MAGIC  NPI ,
# MAGIC  datasource 
# MAGIC from healthcare_rcm_catalog.silver.providers
# MAGIC where is_quarantined=false