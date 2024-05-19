# Databricks notebook source
from pyspark.sql.functions import avg, count, col, month, hour, to_timestamp

def create_schema(schema_name):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

# Apply Gold layer transformation (Business logic)
from pyspark.sql.functions import avg, count, col, month, hour

def perform_aggregations(taxi_df, lookup_df):
    # Join the taxi data with the lookup table
    joined_df = taxi_df.join(lookup_df, taxi_df.pulocationid == lookup_df.locationid)

 # Convert pickup datetime to timestamp using the correct format
    joined_df = joined_df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"), "dd-MM-yyyy HH:mm:ss"))
   

    # Extract month and hour from pickup datetime
    joined_df = joined_df.withColumn("month", month(col("tpep_pickup_datetime"))) \
                         .withColumn("hour", hour(col("tpep_pickup_datetime")))
    
    # Aggregate metrics by Borough and Zone
    aggregated_df = joined_df.groupBy("borough", "zone").agg(
        avg("trip_distance").alias("avg_trip_distance"),
        count("*").alias("total_trips"),
        avg("total_amount").alias("avg_total_amount")
    )
    
    # Aggregate total trips by month
    trips_by_month_df = joined_df.groupBy("borough", "zone", "month").agg(
        count("*").alias("total_trips")
    )
    
    # Aggregate total trips by hour
    trips_by_hour_df = joined_df.groupBy("borough", "zone", "hour").agg(
        count("*").alias("total_trips")
    )
    
    return aggregated_df, trips_by_month_df, trips_by_hour_df

# Write the gold layer tables
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

# Paths to the Silver layer Delta tables
silver_taxi_data_path = "/mnt/02_silver_layer/nyc_taxi_data"
silver_lookup_table_path = "/mnt/02_silver_layer/taxi_zone_lookup"

# Read the Silver layer Delta tables
silver_taxi_data_df = spark.read.format("delta").load(silver_taxi_data_path)
silver_lookup_table_df = spark.read.format("delta").load(silver_lookup_table_path)


# COMMAND ----------

gold_aggregated_df, trips_by_month_df, trips_by_hour_df = perform_aggregations(silver_taxi_data_df, silver_lookup_table_df)


# COMMAND ----------

create_schema("03_gold_layer")
gold_aggregated_data_path = "/mnt/03_gold_layer/aggregated_taxi_data"
gold_trips_by_month_path = "/mnt/03_gold_layer/trips_by_month"
gold_trips_by_hour_path = "/mnt/03_gold_layer/trips_by_hour"

# Write the aggregated DataFrames to the Gold layer as Delta tables
write_delta_table(gold_aggregated_df, gold_aggregated_data_path, "aggregated_taxi_data", "03_gold_layer")
write_delta_table(trips_by_month_df, gold_trips_by_month_path, "trips_by_month", "03_gold_layer")
write_delta_table(trips_by_hour_df, gold_trips_by_hour_path, "trips_by_hour", "03_gold_layer")
