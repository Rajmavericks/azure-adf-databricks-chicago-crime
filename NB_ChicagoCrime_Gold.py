# Databricks notebook source
# Storage account details
storage_account = "dlliveraj"
container = "chicago-crime"
storage_key = dbutils.secrets.get(scope="chicago-crime-scope", key="adls-storage-key")


spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

silver_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/"
gold_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/gold/"

print("Gold Paths configured successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Data to df_gold

# COMMAND ----------

df_silver=spark.read.format("parquet").load(silver_path)
print(f"Record Count : {df_silver.count()}")
print(f"Column Count : {len(df_silver.columns)}")
df_silver.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, when

# Re-apply null handling after reading silver
df_silver = df_silver \
    .fillna({"Location Description": "Unknown"}) \
    .fillna({"Ward": 0, "Community Area": 0}) \
    .fillna({"X Coordinate": 0, "Y Coordinate": 0}) \
    .fillna({"Latitude": 0.0, "Longitude": 0.0})

print("Null handling re-applied")
df_silver.show(5)

# COMMAND ----------

# Aggregation 1 - Crimes by Primary Type
from pyspark.sql.functions import count, desc
df_crimes_by_type = df_silver.groupBy("Primary Type")\
    .agg(count("ID").alias("Total Crimes"))\
    .orderBy(desc("Total Crimes"))

df_crimes_by_type.show(5)


# COMMAND ----------

# Aggregation 2 - Crimes by Year
df_crimes_by_year  = df_silver.groupBy("Year")\
    .agg(count("ID").alias("Total Crimes"))\
    .orderBy(("Year"))

df_crimes_by_year .show(50)



# COMMAND ----------

# Aggregation 3 - Crimes by District
df_crimes_by_district = df_silver \
    .groupBy("District") \
    .agg(count("ID").alias("Total Crimes")) \
    .orderBy(desc("Total Crimes"))

df_crimes_by_district.show(10, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import round, sum

# Aggregation 4 - Arrest Rate by Crime Type
df_arrest_rate = df_silver \
    .groupBy("Primary Type") \
    .agg(
        count("ID").alias("Total Crimes"),
        sum(col("Arrest").cast("integer")).alias("Total Arrests")
    ) \
    .withColumn("Arrest Rate %", round((col("Total Arrests") / col("Total Crimes")) * 100, 2)) \
    .orderBy(desc("Arrest Rate %"))

df_arrest_rate.show(10, truncate=False)

# COMMAND ----------

# Aggregation 5 - Top 10 Crime Location Descriptions
df_top_locations = df_silver \
    .groupBy("Location Description") \
    .agg(count("ID").alias("Total Crimes")) \
    .orderBy(desc("Total Crimes")) \
    .limit(10)

df_top_locations.show(truncate=False)

# COMMAND ----------

# Write all gold aggregations as Parquet

df_crimes_by_type.write.format("parquet") \
    .mode("overwrite") \
    .save(gold_path + "crimes_by_type/")

df_crimes_by_year.write.format("parquet") \
    .mode("overwrite") \
    .save(gold_path + "crimes_by_year/")

df_crimes_by_district.write.format("parquet") \
    .mode("overwrite") \
    .save(gold_path + "crimes_by_district/")

df_arrest_rate.write.format("parquet") \
    .mode("overwrite") \
    .save(gold_path + "arrest_rate_by_type/")

df_top_locations.write.format("parquet") \
    .mode("overwrite") \
    .save(gold_path + "top_crime_locations/")

print("All Gold aggregations written successfully!")

# COMMAND ----------

from pyspark.sql.functions import col

# Helper function to clean column names
def clean_column_names(df):
    new_columns = [c.replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "") for c in df.columns]
    return df.toDF(*new_columns)

# Clean column names for all Gold dataframes
df_crimes_by_type_clean = clean_column_names(df_crimes_by_type)
df_crimes_by_year_clean = clean_column_names(df_crimes_by_year)
df_crimes_by_district_clean = clean_column_names(df_crimes_by_district)
df_arrest_rate_clean = clean_column_names(df_arrest_rate)
df_top_locations_clean = clean_column_names(df_top_locations)

print("Column names cleaned successfully!")

# Verify
print(df_crimes_by_type_clean.columns)
print(df_crimes_by_year_clean.columns)
print(df_crimes_by_district_clean.columns)
print(df_arrest_rate_clean.columns)
print(df_top_locations_clean.columns)

# COMMAND ----------

# Register cleaned Gold tables in Hive Metastore
df_crimes_by_type_clean \
    .write.mode("overwrite").saveAsTable("gold.chicago_crimes_by_type")

df_crimes_by_year_clean \
    .write.mode("overwrite").saveAsTable("gold.chicago_crimes_by_year")

df_crimes_by_district_clean \
    .write.mode("overwrite").saveAsTable("gold.chicago_crimes_by_district")

df_arrest_rate_clean \
    .write.mode("overwrite").saveAsTable("gold.chicago_arrest_rate_by_type")

df_top_locations_clean \
    .write.mode("overwrite").saveAsTable("gold.chicago_top_crime_locations")

print("All Gold tables registered in Hive Metastore!")