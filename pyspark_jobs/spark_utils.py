from pyspark.sql import SparkSession
import config

def get_spark_session(app_name="WeatherProcessing"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", f"{config.MYSQL_JAR},{config.POSTGRES_JAR}")
        .getOrCreate()
    )
