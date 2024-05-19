# Databricks notebook source
from pyspark.sql.functions import col, date_format, to_timestamp

# Create Schema
def create_schema(schema_name):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

# Read Data from a delta Table
def read_delta_table(table_path):
    return spark.read.format("delta").load(table_path)

# Apply Silver Layer transformations
def apply_transformations(df, datetime_columns=[]):
    # Lowercase column names
    df = df.toDF(*[c.lower() for c in df.columns])
    
    # Drop duplicates
    df = df.dropDuplicates()
    
    # Convert datetime columns to specified format
    for column in datetime_columns:
        df = df.withColumn(column, date_format(to_timestamp(col(column), 'M/d/yyyy H:mm'), 'dd-MM-yyyy HH:mm:ss'))
    
    return df

# Write Data to a delta Table
def write_delta_table(df, table_path, table_name, schema_name):
    # Write DataFrame to Delta table
    df.write.format("delta").mode("overwrite").save(table_path)
    
    # Create SQL table for easier querying
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}
        USING DELTA
        LOCATION '{table_path}'
    """)



# COMMAND ----------

# Create the silver_layer schema
create_schema("02_silver_layer")

# Paths to the Bronze layer Delta tables
bronze_taxi_data_path = "/mnt/01_bronze_layer/nyc_taxi_data"
bronze_lookup_table_path = "/mnt/01_bronze_layer/taxi_zone_lookup"

# Read the Bronze layer Delta tables
bronze_taxi_data_df = read_delta_table(bronze_taxi_data_path)
bronze_lookup_table_df = read_delta_table(bronze_lookup_table_path)

# Columns to be formatted as datetime
datetime_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

# Process the DataFrames
silver_taxi_data_df = apply_transformations(bronze_taxi_data_df, datetime_columns)
silver_lookup_table_df = apply_transformations(bronze_lookup_table_df)

# Define paths for the Silver layer Delta tables
silver_taxi_data_path = "/mnt/02_silver_layer/nyc_taxi_data"
silver_lookup_table_path = "/mnt/02_silver_layer/taxi_zone_lookup"

# Write the processed DataFrames to the Silver layer as Delta tables
write_delta_table(silver_taxi_data_df, silver_taxi_data_path, "nyc_taxi_data", "02_silver_layer")
write_delta_table(silver_lookup_table_df, silver_lookup_table_path, "taxi_zone_lookup", "02_silver_layer")


# COMMAND ----------


