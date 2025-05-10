# Databricks notebook source
parquet_path = "abfss://bronze@projhealthcaresa.dfs.core.windows.net/Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center/"
df = spark.read.parquet(parquet_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# Filter rows where code_2_type is 'CPT Codes'
df_filtered = df.filter(col("code_2_type") == "CPT")


# COMMAND ----------

display(df_filtered)

# COMMAND ----------

# List of columns to drop
columns_to_drop = ["code_1", "code_1_type", "code_2_type", "code_3", "code_3_type", "code_4", "code_4_type", "drug_unit_of_measurement", "drug_type_of_measurement","additional_generic_notes","modifiers"] 

# Drop the specified columns
df_filtered = df_filtered.drop(*columns_to_drop)

# Show the result
df_filtered.show()


# COMMAND ----------

# Rename 'code_2' to 'cpt_code'
df_renamed = df_filtered.withColumnRenamed("code_2", "cpt_code")

# Show the updated DataFrame
df_renamed.show()


# COMMAND ----------

display(df_renamed)

# COMMAND ----------

len(df_renamed.columns)

# COMMAND ----------

output_path = "abfss://silver@projhealthcaresa.dfs.core.windows.net/Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center"
df_renamed.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

