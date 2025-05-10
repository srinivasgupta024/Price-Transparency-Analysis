# Databricks notebook source
# HARD CODE YOUR STORAGE KEY HERE (be very careful!)
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

dbutils.widgets.text("hospital_name", "")
hospital_name = dbutils.widgets.get("hospital_name")
print("Running notebook for:", hospital_name)


# COMMAND ----------

from pyspark.sql.types import DoubleType, StringType
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
            if col_name == "hospital_name":
                df = df.withColumn(col_name, F.initcap(F.trim(F.col(col_name))))
            else:
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

    # Format last_updated_on
    if "last_updated_on" in df.columns:
        df = df.withColumn(
            "last_updated_on",
            F.date_format(
                F.coalesce(
                    F.to_date(F.col("last_updated_on"), "MM/dd/yyyy"),
                    F.to_date(F.col("last_updated_on"), "M/d/yyyy"),
                    F.to_date(F.col("last_updated_on"), "MM-dd-yyyy"),
                    F.to_date(F.col("last_updated_on"), "yyyy-MM-dd")
                ),
                "yyyy-MM-dd"
            )
        )

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
        df = df.withColumn("payer_name", F.lower(F.col("payer_name")))
        df = df.withColumn("payer_name", standardize_payer_udf("payer_name"))
        df = df.withColumn("payer_name", F.initcap("payer_name"))

    # Standardize and split hospital_location
    if "hospital_location" in df.columns:
        # Ensure consistent comma spacing
        df = df.withColumn("hospital_location", F.regexp_replace("hospital_location", ";", ","))
        df = df.withColumn("hospital_location", F.regexp_replace("hospital_location", "\\s*,\\s*", ", "))

        # Extract components using regex
        df = df.withColumn("hospital_street", F.regexp_extract("hospital_location", r"^([^,]+)", 1))
        df = df.withColumn("hospital_city", F.regexp_extract("hospital_location", r"^.*?,\\s*([^,]+)", 1))
        df = df.withColumn("hospital_state", F.regexp_extract("hospital_location", r"([A-Z]{2})\\s*\\d{5}(-\\d{4})?$", 1))
        df = df.withColumn("hospital_zip", F.regexp_extract("hospital_location", r"(\\d{5}(?:-\\d{4})?)$", 1))

        # Clean up formatting
        df = df.withColumn("hospital_street", F.initcap("hospital_street"))
        df = df.withColumn("hospital_city", F.initcap("hospital_city"))
        df = df.withColumn("hospital_state", F.concat(F.upper(F.col("hospital_state")), F.lit(",")))

    # Write to Gold
    df.write.format("parquet").mode("overwrite").save(gold_path)
    print(f"✅ Cleaned data written to: {gold_path}")


# COMMAND ----------

clean_hospital_file(hospital_name)