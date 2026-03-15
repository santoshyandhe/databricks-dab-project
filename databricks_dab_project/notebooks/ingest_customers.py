# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
spark = SparkSession.builder.getOrCreate()
data = [
("1","Alice","Johnson","123 Main St","Toronto","ON","M5H1A1"),
("2","Bob","Smith","456 Oak Ave","Hamilton","ON","L8P1A2"),
("3","Carol","White","789 Pine Rd","Vancouver","BC","V6B1A3"),
]
columns = ["id","name","surname","address","city","state","zipcode"]
df = (spark.createDataFrame(data, columns)
.withColumn("ingested_at", current_timestamp()))

df.write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("adb_workspace_dev_7405609302815556.default.customer")
print(f"Ingested {df.count()} rows successfully.")