# import os
# from dotenv import load_dotenv
# from pyspark.sql import SparkSession

# # Load environment variables from .env file
# load_dotenv()

# # MySQL credentials from .env
# MYSQL_HOST = os.getenv("FILESS_MYSQL_HOST")
# MYSQL_PORT = os.getenv("FILESS_MYSQL_PORT")
# MYSQL_USER = os.getenv("FILESS_MYSQL_USER")
# MYSQL_PASSWORD = os.getenv("FILESS_MYSQL_PASSWORD")
# MYSQL_DATABASE = os.getenv("FILESS_MYSQL_DATABASE")
# MYSQL_TABLE = os.getenv("FILESS_MYSQL_TABLE")

# # Initialize SparkSession
# spark = SparkSession.builder \
#     .appName("Fetch MySQL Data") \
#     .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.3.0.jar") \
#     .getOrCreate()

# # JDBC URL
# jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?useSSL=false&serverTimezone=UTC"

# # Read data from MySQL into Spark DataFrame
# df = spark.read \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("driver", "com.mysql.cj.jdbc.Driver") \
#     .option("dbtable", MYSQL_TABLE) \
#     .option("user", MYSQL_USER) \
#     .option("password", MYSQL_PASSWORD) \
#     .load()

# # Show first few rows
# df.show()

# # Stop Spark
# spark.stop()