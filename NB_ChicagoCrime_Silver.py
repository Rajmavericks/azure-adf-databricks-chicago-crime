# Databricks notebook source
# Storage account details
storage_account = "dlliveraj"
container = "chicago-crime"
storage_key = dbutils.secrets.get(scope="chicago-crime-scope", key="adls-storage-key")


spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/"
silver_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/"

print("Silver notebook  Paths configured successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from Bronze!
# MAGIC

# COMMAND ----------

## Read from bronze layer
df_bronze = spark.read.format("parquet")\
       .load(bronze_path)
print(f"Total Records:{df_bronze.count()}")
print(f"Total Columns: {len(df_bronze.columns)}")
df_bronze.printSchema()

# COMMAND ----------

df_bronze.display();

# COMMAND ----------

#Type Casting & Cleaning (Silver)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
from pyspark.sql.functions import to_timestamp, col, trim, to_date
df_silver = df_bronze.withColumn("Date", to_timestamp(col("Date"), "MM/dd/yyyy HH:mm:ss"))\
                   .withColumn("Updated On", to_timestamp(col("Updated On"), "MM/dd/yyyy HH:mm:ss"))\
                   .withColumn("Primary Type", trim(col("Primary Type")))\
                   .withColumn("Description", trim(col("Description")))\
                   .withColumn("Location Description", trim(col("Location Description")))\
                   .drop("Location")
print("Schema after casting:")
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Null Handling

# COMMAND ----------

# DBTITLE 1,Cell 7
# Check nulls before cleaning

# Check nulls before cleaning
print("Null counts before cleaning:")
from pyspark.sql.functions import count, when, isnan



null_counts = df_silver.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_silver.columns
])
null_counts.show(vertical=True)

# COMMAND ----------

from pyspark.sql.functions import col

# Handle nulls
df_silver_clean = df_silver \
    .fillna({"Location Description": "Unknown"}) \
    .fillna({"Ward": 0, "Community Area": 0}) \
    .fillna({"X Coordinate": 0, "Y Coordinate": 0}) \
    .fillna({"Latitude": 0.0, "Longitude": 0.0})

# Verify nulls are gone
print("Null counts after cleaning:")
null_counts_after = df_silver_clean.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_silver_clean.columns
])
null_counts_after.show(vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Silver to ADLS
# MAGIC

# COMMAND ----------

# Write cleaned data to silver layer as Parquet
df_silver.write.mode("overwrite").format("parquet").save(silver_path)
print("Silver layer written successfully")