# Databricks notebook source
!pip install requests

# COMMAND ----------

import requests

# URLs for the CSV files to be downloaded
taxi_data_url = "https://raw.githubusercontent.com/mujahidniaz/databricks-lakehouse-webinar/main/nyc_taxi_data.csv"
lookup_table_url = "https://raw.githubusercontent.com/mujahidniaz/databricks-lakehouse-webinar/main/taxi-zone-lookup.csv"

# Download the NYC taxi data CSV file from the provided URL
response = requests.get(taxi_data_url)
response.raise_for_status()  # Ensure we notice bad responses
taxi_data_content = response.content

# Define the path where the downloaded NYC taxi data CSV file will be saved
taxi_data_file_path = "/dbfs/tmp/nyc_taxi_data.csv"

# Save the downloaded content to a file in DBFS
with open(taxi_data_file_path, "wb") as file:
    file.write(taxi_data_content)

# Download the taxi zone lookup CSV file from the provided URL
response = requests.get(lookup_table_url)
response.raise_for_status()  # Ensure we notice bad responses
lookup_table_content = response.content

# Define the path where the downloaded taxi zone lookup CSV file will be saved
lookup_table_file_path = "/dbfs/tmp/taxi-zone-lookup.csv"

# Save the downloaded content to a file in DBFS
with open(lookup_table_file_path, "wb") as file:
    file.write(lookup_table_content)


# COMMAND ----------

# Read the NYC taxi data CSV file into a DataFrame
taxi_data_file_path = "dbfs:/tmp/nyc_taxi_data.csv"
taxi_data_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(taxi_data_file_path)

# Read the taxi zone lookup CSV file into a DataFrame
lookup_table_file_path = "dbfs:/tmp/taxi-zone-lookup.csv"
lookup_table_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(lookup_table_file_path)

# Show a few rows of the DataFrames to verify
display(taxi_data_df)
display(lookup_table_df)



# COMMAND ----------

# Define paths for the Bronze layer Delta tables
bronze_taxi_data_path = "/mnt/01_bronze_layer/nyc_taxi_data"
bronze_lookup_table_path = "/mnt/01_bronze_layer/taxi_zone_lookup"

# Write the NYC taxi data DataFrame to the Bronze layer as a Delta table
taxi_data_df.write.format("delta").mode("overwrite").save(bronze_taxi_data_path)

# Write the taxi zone lookup DataFrame to the Bronze layer as a Delta table
lookup_table_df.write.format("delta").mode("overwrite").save(bronze_lookup_table_path)

# Create SQL tables for easier querying (optional)
# Create the bronze_layer schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS 01_bronze_layer")
spark.sql("CREATE TABLE IF NOT EXISTS 01_bronze_layer.nyc_taxi_data USING DELTA LOCATION '{}'".format(bronze_taxi_data_path))
spark.sql("CREATE TABLE IF NOT EXISTS 01_bronze_layer.taxi_zone_lookup USING DELTA LOCATION '{}'".format(bronze_lookup_table_path))

