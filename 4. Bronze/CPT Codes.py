# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

cpt_codes_df = spark.read.csv("/mnt/rcmdatalake03/landing/cptcodes/*.csv", header=True)

for col in cpt_codes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cpt_codes_df = cpt_codes_df.withColumnRenamed(col, new_col)
cpt_codes_df.createOrReplaceTempView("cptcodes")

# COMMAND ----------

display(cpt_codes_df)

# COMMAND ----------

cpt_codes_df.write.format("parquet").mode("overwrite").save("/mnt/rcmdatalake03/bronze/cpt_codes/")