# Databricks notebook source
# MAGIC %sql
# MAGIC -- 1.1 Create (if not exists) the database for dims & facts
# MAGIC CREATE DATABASE IF NOT EXISTS warehouse
# MAGIC LOCATION 'abfss://warehouse@projhealthcaresa.dfs.core.windows.net/';
# MAGIC
# MAGIC -- 1.2 Create (if not exists) the database for Gold summaries
# MAGIC CREATE DATABASE IF NOT EXISTS gold
# MAGIC LOCATION 'abfss://gold@projhealthcaresa.dfs.core.windows.net/';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2.1 Hospital dimension
# MAGIC CREATE TABLE IF NOT EXISTS warehouse.dim_hospital
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_hospital_delta/';
# MAGIC
# MAGIC -- 2.2 Procedure dimension
# MAGIC CREATE TABLE IF NOT EXISTS warehouse.dim_procedure
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_procedure_delta/';
# MAGIC
# MAGIC -- 2.3 Payer dimension
# MAGIC CREATE TABLE IF NOT EXISTS warehouse.dim_payer
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_payer_delta/';
# MAGIC
# MAGIC -- 2.4 Plan dimension
# MAGIC CREATE TABLE IF NOT EXISTS warehouse.dim_plan
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_plan_delta/';
# MAGIC
# MAGIC -- 2.5 Setting dimension
# MAGIC CREATE TABLE IF NOT EXISTS warehouse.dim_setting
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_setting_delta/';
# MAGIC
# MAGIC -- 2.6 Methodology dimension
# MAGIC CREATE TABLE IF NOT EXISTS warehouse.dim_methodology
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_methodology_delta/';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS warehouse.fact_charge
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://warehouse@projhealthcaresa.dfs.core.windows.net/fact_charge_delta/';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4.1 Hospital Summary
# MAGIC CREATE TABLE IF NOT EXISTS gold.hospital_summary
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@projhealthcaresa.dfs.core.windows.net/hospital_summary_delta/';
# MAGIC
# MAGIC -- 4.2 Procedure Summary
# MAGIC CREATE TABLE IF NOT EXISTS gold.procedure_summary
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@projhealthcaresa.dfs.core.windows.net/procedure_summary_delta/';
# MAGIC
# MAGIC -- 4.3 Payer Summary
# MAGIC CREATE TABLE IF NOT EXISTS gold.payer_summary
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@projhealthcaresa.dfs.core.windows.net/payer_summary_delta/';
# MAGIC
# MAGIC -- 4.4 Hospital × Procedure Cross-Tab
# MAGIC CREATE TABLE IF NOT EXISTS gold.hospital_procedure
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@projhealthcaresa.dfs.core.windows.net/hospital_procedure_delta/';
# MAGIC
# MAGIC -- 4.5 Setting Summary
# MAGIC CREATE TABLE IF NOT EXISTS gold.setting_summary
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@projhealthcaresa.dfs.core.windows.net/setting_summary_delta/';
# MAGIC
# MAGIC -- 4.6 Methodology Summary
# MAGIC CREATE TABLE IF NOT EXISTS gold.methodology_summary
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@projhealthcaresa.dfs.core.windows.net/methodology_summary_delta/';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED warehouse.dim_hospital;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.hospital_summary LIMIT 10;
# MAGIC

# COMMAND ----------

