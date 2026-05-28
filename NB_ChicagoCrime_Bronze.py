# Databricks notebook source
# Storage account details
storage_account = "dlliveraj"
container = "chicago-crime"
storage_key = dbutils.secrets.get(scope="chicago-crime-scope", key="adls-storage-key")


spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

raw_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/raw/chicago_crime_raw.csv"
bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/"

print("Paths configured successfully")

# COMMAND ----------

df_raw=spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("multiLine", "true") \
    .option("escape", '"') \
    .load(raw_path)
print(f"Total Records:{df_raw.count()}")
print(f"Total Columns: {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to bronze as Parquet — no transformations

# COMMAND ----------

df_raw=df_raw.limit(50000)
df_raw.write.mode("overwrite").format("parquet").save(bronze_path)
print("Data written to Bronze")