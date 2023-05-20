# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col


# Define the schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# Create the DataFrame
data = [("John", 25, "New York"),
        ("Alice", 30, "San Francisco"),
        ("Bob", 22, "Chicago")]

df = spark.createDataFrame(data, schema)

# Filter the DataFrame
filtered_df = df.filter(col("age") >= 18)

# Show the filtered DataFrame
filtered_df.show()




