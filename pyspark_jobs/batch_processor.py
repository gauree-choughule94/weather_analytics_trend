from datetime import timedelta
import pyspark.sql.functions as F
import config

def fill_missing_with_mode(df):
    # Drop columns with all nulls
    df = df.drop("sealevelpressure", "snow", "snowdepth", "preciptype")
    
    for col_name, dtype in df.dtypes:
        mode_value = (
            df.groupBy(col_name)
              .count()
              .orderBy(F.desc("count"))
              .first()
        )
        if mode_value and mode_value[0] is not None:
            # Handle timestamp columns
            if dtype == "timestamp":
                df = df.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNull(), F.lit(mode_value[0])).otherwise(F.col(col_name))
                )
            else:
                df = df.fillna({col_name: mode_value[0]})
        
        # Round numeric columns to 2 decimal places
        if dtype in ["double", "float", "decimal"]:
            df = df.withColumn(col_name, F.round(F.col(col_name), 2))
        elif dtype in ["int", "bigint"]:
            # No rounding needed for pure integers, but can cast if needed
            pass

    return df

def get_date_range(spark):
    mysql_url = f"jdbc:mysql://{config.MYSQL_HOST}:{config.MYSQL_PORT}/{config.MYSQL_DATABASE}"
    df_all_dates = (
        spark.read.format("jdbc")
        .option("url", mysql_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", config.MYSQL_TABLE)
        .option("user", config.MYSQL_USER)
        .option("password", config.MYSQL_PASSWORD)
        .load()
        .select(F.min(F.col(config.DATE_COLUMN)).alias("min_date"),
                F.max(F.col(config.DATE_COLUMN)).alias("max_date"))
        .collect()[0]
    )
    return df_all_dates["min_date"], df_all_dates["max_date"]

def process_batches(spark):
    mysql_url = f"jdbc:mysql://{config.MYSQL_HOST}:{config.MYSQL_PORT}/{config.MYSQL_DATABASE}"
    pg_url = f"jdbc:postgresql://{config.PG_HOST}:{config.PG_PORT}/{config.PG_DATABASE}"

    min_date, max_date = get_date_range(spark)
    total_days = (max_date - min_date).days
    total_batches = (total_days // config.DAYS_PER_BATCH) + 1

    print(f"Total days: {total_days}, Total batches: {total_batches}")

    for batch_number in range(total_batches):
        start_date = min_date + timedelta(days=batch_number * config.DAYS_PER_BATCH)
        end_date = min_date + timedelta(days=(batch_number + 1) * config.DAYS_PER_BATCH)

        mysql_query = f"""
            (SELECT * FROM {config.MYSQL_TABLE}
             WHERE {config.DATE_COLUMN} >= '{start_date:%Y-%m-%d %H:%M:%S}'
               AND {config.DATE_COLUMN} < '{end_date:%Y-%m-%d %H:%M:%S}'
             ORDER BY {config.DATE_COLUMN} ASC) AS tmp
        """

        df_batch = (
            spark.read.format("jdbc")
            .option("url", mysql_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", mysql_query)
            .option("user", config.MYSQL_USER)
            .option("password", config.MYSQL_PASSWORD)
            .load()
        )

        if df_batch.count() == 0:
            print(f"Batch {batch_number+1}: No data, skipping.")
            continue

        # Fill missing values, drop null-only cols, round numbers
        df_cleaned = fill_missing_with_mode(df_batch)

        df_transformed = df_cleaned \
            .withColumn("avg_temp", F.round((F.col("tempmax") + F.col("tempmin")) / 2, 2)) \
            .withColumn("temp_range", F.round(F.col("tempmax") - F.col("tempmin"), 2)) \
            .withColumn("feelslike_diff", F.round(F.col("feelslike") - F.col("temp"), 2)) \
            .withColumn("weather_category",
                        F.when(F.col("conditions").contains("Rain"), "Rainy")
                        .when(F.col("conditions").contains("Snow"), "Snowy")
                        .when(F.col("conditions").contains("Clear"), "Sunny")
                        .when(F.col("conditions").contains("Cloud"), "Cloudy")
                        .otherwise("Other")) \
            .withColumn("day_of_week", F.date_format(F.to_timestamp("datetime"), "E")) \
            .withColumn("daylight_hours",
                        F.round((F.unix_timestamp("sunset") - F.unix_timestamp("sunrise")) / 3600, 2))

        df_transformed.write.format("jdbc") \
            .option("url", pg_url) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "city_weather") \
            .option("user", config.PG_USER) \
            .option("password", config.PG_PASSWORD) \
            .mode("append") \
            .save()

        print(f"Batch {batch_number+1} written to PostgreSQL.")
