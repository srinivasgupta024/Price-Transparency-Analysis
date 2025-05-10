# Databricks notebook source
# HARD CODE YOUR STORAGE KEY HERE (be very careful!)
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# Set input/output paths
hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://silver@projhealthcare.dfs.core.windows.net/{hospital}/"
gold_base_path = "abfss://gold@projhealthcare.dfs.core.windows.net"

# Load Parquet
df = spark.read.parquet(silver_path)

# Show sample
df.display()


# COMMAND ----------

df.printSchema()
from pyspark.sql import functions as F

# Count nulls per column
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()



# COMMAND ----------

df.count(), df.dropDuplicates().count()  # Full row duplicates


# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower
string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == 'string']
for c in string_cols:
    df = df.withColumn(c, trim(lower(col(c))))


# COMMAND ----------

df.describe().show()

numeric_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ['int', 'double', 'float', 'bigint']]
for col_name in numeric_cols:
    df.select(col_name).summary("min", "max", "mean", "stddev").show()


# COMMAND ----------

