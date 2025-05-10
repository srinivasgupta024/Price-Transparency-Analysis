# Databricks notebook source
# HARD CODE YOUR STORAGE KEY HERE (be very careful!)
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

from pyspark.sql.types import DoubleType, StringType, DateType
from pyspark.sql import functions as F
from datetime import datetime

def clean_hospital_file(hospital_name):
    silver_path = f"abfss://silver@projhealthcare.dfs.core.windows.net/{hospital_name}/"
    gold_path = f"abfss://stage@projhealthcare.dfs.core.windows.net/{hospital_name}/"

    print(f"\n🚀 Processing hospital: {hospital_name}")

    # Load
    df = spark.read.parquet(silver_path)
    print(f"✅ Rows loaded: {df.count()}")

    # Drop exact duplicates
    df = df.dropDuplicates()

    # Trim string columns
    for col_name, dtype in df.dtypes:
        if dtype == 'string':
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # Cast numeric fields
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

    # # Cast last_updated_on to date
    # if "last_updated_on" in df.columns:
    #     df = df.withColumn("last_updated_on", F.to_date("last_updated_on", "MM/dd/yyyy"))

    # Standardize payer_name
    payer_mapping = {
        "blue cross blue shield": "blue cross",
        "bcbs": "blue cross",
        "aetna health": "aetna",
        "united healthcare": "unitedhealth",
        "uhc": "unitedhealth",
        "cigna health": "cigna",
        "kaiser permanente": "kaiser"
    }

    def standardize_payer(name):
        if name is None:
            return "unknown"
        name = name.lower()
        for key in payer_mapping:
            if key in name:
                return payer_mapping[key]
        return name

    from pyspark.sql.functions import udf
    standardize_payer_udf = F.udf(standardize_payer, StringType())

    if "payer_name" in df.columns:
        df = df.withColumn("payer_name", F.trim(F.lower(F.col("payer_name"))))
        df = df.withColumn("payer_name", standardize_payer_udf("payer_name"))

    # Fill nulls
    for col_name, dtype in df.dtypes:
        if dtype == 'string':
            df = df.withColumn(col_name, F.when(F.col(col_name).isNull(), 'NA').otherwise(F.col(col_name)))
        elif dtype in ['double', 'int', 'bigint', 'float']:
            df = df.withColumn(col_name, F.when(F.col(col_name).isNull(), 0).otherwise(F.col(col_name)))

    # # Add metadata
    # df = df.withColumn("hospital_name", F.lit(hospital_name))
    # df = df.withColumn("ingested_on", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # Show null counts
    print("📊 Null count per column:")
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
    null_counts.show(truncate=False)

    # Write to Gold
    df.write.format("parquet").mode("overwrite").save(gold_path)
    print(f"✅ Cleaned data written to: {gold_path}")


# COMMAND ----------

clean_hospital_file("John_Muir_Health_Walnut_Creek_Medical_Center")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Mercy_General_Hospital

# Mercy_Hospital_of_Folsom

# Mercy_San_Juan_Medical_Center

# Methodist_Hospital_of_Sacramento

# Saint_Francis_Memorial_Hospital

# San_Ramon_Regional_Medical_Center

# Sequoia_Hospital

# St.Mary's_Medical_Center

# Woodland_Memorial_Hospital

# COMMAND ----------

# hospital_list = [
    # "Contra_Costa_Regional_Medical_Center",
    # "Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center",
    # "Scripps_Green_Hospital",
    # "Scripps_Memorial_Hospital_Encinitas",
    # "Scripps_Memorial_Hospital_La_Jolla",
    # "Scripps_Mercy_Hospital_Chula_Vista",
    # "Scripps_Mercy_Hospital_San_Diego",
    # "Sharp_Chula_Vista_Medical_Center",
    # "Sharp_Coronado_Hospital",
    # "Sharp_Grossmont_Hospital",
    # "Sharp_Memorial_Hospital"

    # "Kaiser_Foundation_Hospital_Walnut_Creek",
    # "Kaiser_Foundation_Hospital_Vallejo",
    # "Mercy_General_Hospital",
    # "Mercy_Hospital_of_Folsom",
    # "Mercy_San_Juan_Medical_Center",
    # "Methodist_Hospital_of_Sacramento",
    # "Saint_Francis_Memorial_Hospital",
    # "San_Ramon_Regional_Medical_Center",
    # "Sequoia_Hospital",
    # "St.Mary's_Medical_Center",
    # "Woodland_Memorial_Hospital",
    # "John_Muir_Health_Walnut_Creek_Medical_Center",
    # "John_Muir_Medical_Center_Concord_Campus"

]

# for h in hospital_list:
#     process_hospital_data(h)


# COMMAND ----------

# Get list of folders from Silver container
silver_base_path = "abfss://silver@projhealthcare.dfs.core.windows.net/"

hospital_folders = [f.name.strip('/') for f in dbutils.fs.ls(silver_base_path) if f.name.endswith('/')]

# Loop through each and clean
for hospital in hospital_folders:
    try:
        clean_hospital_file(hospital)
    except Exception as e:
        print(f"❌ Failed to process {hospital}: {e}")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Set input/output paths
# hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://stage@projhealthcare.dfs.core.windows.net/John_Muir_Health_Walnut_Creek_Medical_Center/"
gold_base_path = "abfss://silver@projhealthcare.dfs.core.windows.net/Sharp_Chula_Vista_Medical_Center/"

# Load Parquet
df = spark.read.format("parquet").load(silver_path)

# Show sample
df.display()

# COMMAND ----------

df.columns

# COMMAND ----------

# Load Parquet
df = spark.read.format("parquet").load(gold_base_path)

# Show sample
df.display()

# COMMAND ----------

df.count(),df.dropDuplicates().count()

# COMMAND ----------

