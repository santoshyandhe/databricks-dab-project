# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read raw customer table
df = spark.table("adb_workspace_dev_7405609302815556.default.customer")

# Apply transformations
from pyspark.sql.functions import concat, lit, upper, current_timestamp

df_transformed = df.select(
    "id",
    "name",
    "surname",
    concat("name", lit(" "), "surname").alias("full_name"),
    "address",
    "city",
    "state",
    "zipcode",
    upper("city").alias("city_upper"),
    current_timestamp().alias("transformed_at")
).filter("id IS NOT NULL")

# Write to transformed table
df_transformed.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("adb_workspace_dev_7405609302815556.default.customer_transformed")

print(f"Transformed {df_transformed.count()} rows into adb_workspace_dev_7405609302815556.default.customer_transformed")