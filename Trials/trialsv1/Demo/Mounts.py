# Databricks notebook source
# 1. Define your storage/account info
storage_account = "projhealthcaresa"
container = "warehouse"   # your warehouse container
account_key = "GxS51LC9kLNa4wK+b9BcUuYEwaF1IFZ3SyM3iS0PH0siEezoPdrd8/j73Aw7nSSNlMNCOUAZmJRK+ASth1N2jQ=="

# 2. Mount using the Blob endpoint (wasbs://)
dbutils.fs.mount(
  source = f"wasbs://{container}@{storage_account}.blob.core.windows.net/",
  mount_point = "/mnt/warehouse",
  extra_configs = {
    f"fs.azure.account.key.{storage_account}.blob.core.windows.net": account_key
  }
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/mnt/warehouse/dim_plan_delta/`

# COMMAND ----------

