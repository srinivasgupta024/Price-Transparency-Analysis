# Databricks notebook source
# HARD CODE YOUR STORAGE KEY HERE (be very careful!)
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

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

clean_hospital_file("Kaiser_Foundation_Hospital_SanRafael")

# COMMAND ----------

hospital_list = [
    "Contra_Costa_Regional_Medical_Center",
    "Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center",
    "Scripps_Green_Hospital",
    "Scripps_Memorial_Hospital_Encinitas",
    "Scripps_Memorial_Hospital_La_Jolla",
    "Scripps_Mercy_Hospital_Chula_Vista",
    "Scripps_Mercy_Hospital_San_Diego",
    "Sharp_Chula_Vista_Medical_Center",
    "Sharp_Coronado_Hospital",
    "Sharp_Grossmont_Hospital",
    "Sharp_Memorial_Hospital",
    "Kaiser_Foundation_Hospital_Walnut_Creek",
    "Kaiser_Foundation_Hospital_Vallejo",
    "Mercy_General_Hospital",
    "Mercy_Hospital_of_Folsom",
    "Mercy_San_Juan_Medical_Center",
    "Methodist_Hospital_of_Sacramento",
    "Saint_Francis_Memorial_Hospital",
    "San_Ramon_Regional_Medical_Center",
    "Sequoia_Hospital",
    "St.Mary's_Medical_Center",
    "Woodland_Memorial_Hospital",
    "John_Muir_Health_Walnut_Creek_Medical_Center",
    "John_Muir_Medical_Center_Concord_Campus"
]

for h in hospital_list:
    clean_hospital_file(h)


# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def drop_location_parts(df):
    return df.drop("hospital_street", "hospital_city", "hospital_state", "hospital_zip")

hospital_list = [
    "Contra_Costa_Regional_Medical_Center",
    "Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center",
    "Scripps_Green_Hospital",
    "Scripps_Memorial_Hospital_Encinitas",
    "Scripps_Memorial_Hospital_La_Jolla",
    "Scripps_Mercy_Hospital_Chula_Vista",
    "Scripps_Mercy_Hospital_San_Diego",
    "Sharp_Chula_Vista_Medical_Center",
    "Sharp_Coronado_Hospital",
    "Sharp_Grossmont_Hospital",
    "Sharp_Memorial_Hospital",
    "Kaiser_Foundation_Hospital_Walnut_Creek",
    "Kaiser_Foundation_Hospital_Vallejo",
    "Mercy_General_Hospital",
    "Mercy_Hospital_of_Folsom",
    "Mercy_San_Juan_Medical_Center",
    "Methodist_Hospital_of_Sacramento",
    "Saint_Francis_Memorial_Hospital",
    "San_Ramon_Regional_Medical_Center",
    "Sequoia_Hospital",
    "St.Mary's_Medical_Center",
    "Woodland_Memorial_Hospital",
    "John_Muir_Health_Walnut_Creek_Medical_Center",
    "John_Muir_Medical_Center_Concord_Campus"
]

for hospital_name in hospital_list:
    silver_path = f"abfss://stage@projhealthcare.dfs.core.windows.net/{hospital_name}/"
    stage_path = f"abfss://stage@projhealthcare.dfs.core.windows.net/{hospital_name}/"

    df = spark.read.format("parquet").load(silver_path)
    df_clean = drop_location_parts(df)

    df_clean.write.mode("overwrite").format("parquet").save(stage_path)


# COMMAND ----------



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

# Set input/output paths
# hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://stage@projhealthcare.dfs.core.windows.net/Sharp_Chula_Vista_Medical_Center/"
gold_base_path = "abfss://silver@projhealthcare.dfs.core.windows.net/Sharp_Chula_Vista_Medical_Center/"

# Load Parquet
df = spark.read.format("parquet").load(silver_path)

# Show sample
df.display()

# COMMAND ----------

# Load Parquet
df = spark.read.format("parquet").load(silver_path)

# Show sample
df.count()

# COMMAND ----------



# COMMAND ----------

df.count(),df.dropDuplicates().count()

# COMMAND ----------

