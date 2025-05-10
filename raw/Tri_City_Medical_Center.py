# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

# Set input/output paths
# hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://demo@projhealthcare.dfs.core.windows.net/Tri_City_Medical_Center.csv"


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, expr, trim, monotonically_increasing_id

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the CSV file with no header
raw_df = spark.read.option("header", False).csv(silver_path)

# Save first 2 rows in a separate DataFrame
meta_rows = raw_df.limit(2)

# Get the 3rd row as the new header
columns_row = raw_df.limit(3).collect()[2]
columns = [str(cell) for cell in columns_row]

# Create the schema using all string types
from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([StructField(col_name, StringType(), True) for col_name in columns])

# Extract data starting from row 4
data_rdd = raw_df.rdd.zipWithIndex().filter(lambda row: row[1] > 2).map(lambda row: [str(cell) for cell in row[0]])
data_df = spark.createDataFrame(data_rdd, schema=schema)

# Keep original metadata columns
base_columns = [c for c in [
    "description", "code|1", "code|1|type", "code|2", "code|2|type", "code|3", "code|3|type",
    "code|4", "code|4|type", "code|5", "code|5|type", "code|6", "code|6|type",
    "modifiers", "setting", "drug_unit_of_measurement", "drug_type_of_measurement",
    "standard_charge|gross", "standard_charge|discounted_cash",
    "standard_charge|min", "standard_charge|max"
] if c in data_df.columns]  # Only keep base columns that exist in schema

payer_columns = [c for c in data_df.columns if c not in base_columns and c != "row_id"]

# Add row_id to join later
data_df = data_df.withColumn("row_id", monotonically_increasing_id())

# Melt the payer-specific columns using select + expr to avoid unresolved column error
stack_expr = ",".join([f"'{col}', `{col}`" for col in payer_columns])
melted_df = data_df.select(
    "row_id",
    *base_columns,
    expr(f"stack({len(payer_columns)}, {stack_expr}) as (payer_col, value)")
)

# Extract components from column names
split_col = split("payer_col", "\\|")
melted_df = melted_df.withColumn("charge_type", split_col.getItem(0)) \
                       .withColumn("payer", split_col.getItem(1)) \
                       .withColumn("plan", split_col.getItem(2)) \
                       .withColumn("attribute", split_col.getItem(3))

# Pivot to get one row per payer/plan/row_id with all charge attributes
pivoted = melted_df.groupBy("row_id", "payer", "plan").pivot("attribute").agg(expr("first(value)"))

# Join back with the base data
base_df = data_df.select(["row_id"] + base_columns)
final_df = base_df.join(pivoted, on="row_id", how="left").drop("row_id")

# Reorder final columns
final_columns = base_columns + [
    "payer", "plan", "negotiated_dollar", "negotiated_percentage", "negotiated_algorithm",
    "estimated_amount", "methodology", "additional_payer_notes"
]
final_columns = [c for c in final_columns if c in final_df.columns]
final_df = final_df.select(*final_columns)

final_df.show(truncate=False)


# COMMAND ----------

display(final_df)

# COMMAND ----------

from pyspark.sql.functions import col
demo = final_df.filter(col("code|1") == "10060")
display(demo)

# COMMAND ----------

