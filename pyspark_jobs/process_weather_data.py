import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime, timedelta

# Load environment variables
env_path = Path("/home/neosoft/Desktop/wheather_analytics_trends/.env")
load_dotenv(dotenv_path=env_path)
# MySQL credentials
MYSQL_HOST = os.getenv("FILESS_MYSQL_HOST")
MYSQL_PORT = os.getenv("FILESS_MYSQL_PORT")
MYSQL_USER = os.getenv("FILESS_MYSQL_USER")
MYSQL_PASSWORD = os.getenv("FILESS_MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("FILESS_MYSQL_DATABASE")
MYSQL_TABLE = os.getenv("FILESS_MYSQL_TABLE")

# PostgreSQL credentials
PG_HOST = "localhost"
PG_PORT =  "5433"
PG_DATABASE = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# ---- Config ----
DAYS_PER_BATCH = 50
DATE_COLUMN = "datetime"

spark = SparkSession.builder \
    .appName("Incremental_Weather_Processing") \
    .config("spark.jars", "infra/pyspark_apps/jars/mysql-connector-j-8.3.0.jar,infra/pyspark_apps/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# MySQL JDBC URL
mysql_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

# --- Step 1: Get min/max datetime in MySQL ---
df_all_dates = spark.read.format("jdbc") \
    .option("url", mysql_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", MYSQL_TABLE) \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .load() \
    .select(F.min(F.col(DATE_COLUMN)).alias("min_date"),
            F.max(F.col(DATE_COLUMN)).alias("max_date")) \
    .collect()[0]
print("#############", df_all_dates)
min_date = df_all_dates["min_date"]
max_date = df_all_dates["max_date"]

print(f"Min date in source: {min_date}, Max date in source: {max_date}")

# Calculate total number of batches
total_days = (max_date - min_date).days
total_batches = (total_days // DAYS_PER_BATCH) + 1

print(f"Total days: {total_days}, Total batches: {total_batches}")

pg_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

for batch_number in range(total_batches):
    print(f"Processing batch {batch_number + 1} / {total_batches}")

    # Calculate batch date range
    start_date = min_date + timedelta(days=batch_number * DAYS_PER_BATCH)
    end_date = min_date + timedelta(days=(batch_number + 1) * DAYS_PER_BATCH)

    # Format dates as string for MySQL query
    start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

    mysql_query = f"""
        (SELECT * FROM {MYSQL_TABLE}
         WHERE {DATE_COLUMN} >= '{start_date_str}'
           AND {DATE_COLUMN} < '{end_date_str}'
         ORDER BY {DATE_COLUMN} ASC) AS tmp
    """

    # Read batch data
    df_batch = spark.read.format("jdbc") \
        .option("url", mysql_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", mysql_query) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .load()

    count = df_batch.count()
    print(f"Fetched {count} records for batch {batch_number + 1}")

    if count == 0:
        print("No data in this batch, skipping.")
        continue

    # Transform data
    df_transformed = df_batch \
        .withColumn("avg_temp", (F.col("tempmax") + F.col("tempmin")) / 2) \
        .withColumn("temp_range", F.col("tempmax") - F.col("tempmin")) \
        .withColumn("feelslike_diff", F.col("feelslike") - F.col("temp")) \
        .withColumn("weather_category",
                    F.when(F.col("conditions").contains("Rain"), "Rainy")
                     .when(F.col("conditions").contains("Snow"), "Snowy")
                     .when(F.col("conditions").contains("Clear"), "Sunny")
                     .when(F.col("conditions").contains("Cloud"), "Cloudy")
                     .otherwise("Other")) \
        .withColumn("day_of_week", F.date_format(F.to_timestamp("datetime"), "E")) \
        .withColumn("daylight_hours",
                    (F.unix_timestamp("sunset") - F.unix_timestamp("sunrise")) / 3600)
    
    print("Columns in DataFrame:", df_transformed.columns)

    # Write batch to PostgreSQL     # .option("dbtable", "processed_weather_data") \
    try:
        df_transformed.write.format("jdbc") \
            .option("url", pg_url) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "mumbai_weather") \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .mode("overwrite") \
            .save()
        print(f"Batch {batch_number + 1} successfully written to PostgreSQL!")
    except Exception as e:
        print(f"Failed to write batch {batch_number + 1} to PostgreSQL:", e)

spark.stop()
