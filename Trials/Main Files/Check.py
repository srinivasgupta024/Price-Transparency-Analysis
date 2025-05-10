# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

# Set input/output paths
# hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://stage@projhealthcare.dfs.core.windows.net/Sharp_Grossmont_Hospital/"
# Load Parquet
df = spark.read.format("parquet").load(silver_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col
demo = df.filter(col("cpt_code") == "11983")
demo = demo.dropDuplicates()
display(demo)

# COMMAND ----------

# Set input/output paths
# hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://stage@projhealthcare.dfs.core.windows.net/Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center/"
# Load Parquet
df = spark.read.format("parquet").load(silver_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col
demo = df.filter(col("cpt_code") == "19000")
display(demo)

# COMMAND ----------

