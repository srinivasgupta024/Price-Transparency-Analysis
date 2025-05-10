# Databricks notebook source


# Set input/output paths
hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://silver@projhealthcare.dfs.core.windows.net/{hospital}/"
gold_base_path = "abfss://stage@projhealthcare.dfs.core.windows.net"

# Load Parquet
df = spark.read.parquet(silver_path)

# Show sample
df.display()


# COMMAND ----------

df = df.withColumn("last_updated_on", F.to_date("last_updated_on", "MM/dd/yyyy"))
display(df)

# COMMAND ----------

from pyspark.sql import functions as F
# Fill numeric nulls with 0
numeric_cols = [
    "standard_charge_gross",
    "standard_charge_discounted_cash",
    "standard_charge_negotiated_dollar",
    "standard_charge_negotiated_percentage",
    "estimated_amount",
    "standard_charge_min",
    "standard_charge_max"
]

for col_name in numeric_cols:
    if col_name in df.columns:
        df = df.withColumn(col_name, F.when(F.col(col_name).isNull(), 0).otherwise(F.col(col_name)))

# Fill string nulls with 'NA'
from pyspark.sql.types import StringType

for col_name, dtype in df.dtypes:
    if dtype == 'string':
        df = df.withColumn(col_name, F.when(F.col(col_name).isNull(), 'NA').otherwise(F.col(col_name)))


# COMMAND ----------

df.count(), df.dropDuplicates().count()  # Full row duplicates


# COMMAND ----------

from pyspark.sql.types import DoubleType, DateType
from pyspark.sql import functions as F
from datetime import datetime

def clean_hospital_file(hospital_name):
    silver_path = f"abfss://silver@projhealthcare.dfs.core.windows.net/{hospital_name}/"
    gold_path = f"abfss://gold@projhealthcare.dfs.core.windows.net/{hospital_name}/"

    print(f"\n🚀 Processing hospital: {hospital_name}")

    # Load
    df = spark.read.parquet(silver_path)
    print(f"✅ Rows loaded: {df.count()}")

    # Remove duplicates
    df = df.dropDuplicates()
    print(f"✅ After Dropped Rows : {df.count()}")
    
    # Trim strings
    for col_name in df.columns:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # Cast numeric columns
    numeric_cols = [
        "standard_charge_gross",
        "standard_charge_discounted_cash",
        "standard_charge_negotiated_dollar",
        "standard_charge_negotiated_percentage",
        "estimated_amount",
        "standard_charge_min",
        "standard_charge_max"
    ]
    for col_name in numeric_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    # Cast last_updated_on to DateType
    if "last_updated_on" in df.columns:
        df = df.withColumn("last_updated_on", F.to_date("last_updated_on", "yyyy-MM-dd"))





# COMMAND ----------

clean_hospital_file("Sharp_Coronado_Hospital")


# COMMAND ----------

    # Handle basic outliers (e.g., negative values)
    for col_name in numeric_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) < 0, None).otherwise(F.col(col_name))
            )

    # Fill some key nulls with placeholders
    df = df.fillna({
        "payer_name": "Unknown",
        "plan_name": "Unknown",
        "standard_charge_methodology": "Not Provided"
    })

# COMMAND ----------


    # Write cleaned Delta file
    df.write.format("parquet").mode("overwrite").save(gold_path)
    print(f"✅ Cleaned data written to: {gold_path}")
