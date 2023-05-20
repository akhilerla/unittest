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



# COMMAND ----------

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

class SparkUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a SparkSession
        cls.spark = SparkSession.builder.getOrCreate()

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

        cls.df = cls.spark.createDataFrame(data, schema)

    def test_filter_age_greater_equal(self):
        # Filter the DataFrame
        filtered_df = self.df.filter(col("age") >= 18)

        # Assert the expected number of rows
        expected_rows = 3
        actual_rows = filtered_df.count()
        self.assertEqual(expected_rows, actual_rows)

        # Assert the filtered DataFrame contains the expected rows
        expected_data = [("John", 25, "New York"),
                        ("Alice", 30, "San Francisco"),
                        ("Bob", 22, "Chicago")]
        actual_data = filtered_df.collect()
        self.assertEqual(expected_data, actual_data)


# create a test suite  for test_class using  loadTestsFromTestCase()
suite = unittest.TestLoader().loadTestsFromTestCase(SparkUnitTest)
unittest.TextTestRunner(verbosity=2).run(suite)
