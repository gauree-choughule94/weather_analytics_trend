import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env file
env_path = Path("/home/neosoft/Desktop/wheather_analytics_trends/infra/.env")
load_dotenv(dotenv_path=env_path)

# MySQL credentials
MYSQL_HOST = os.getenv("FILESS_MYSQL_HOST")
MYSQL_PORT = os.getenv("FILESS_MYSQL_PORT")
MYSQL_USER = os.getenv("FILESS_MYSQL_USER")
MYSQL_PASSWORD = os.getenv("FILESS_MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("FILESS_MYSQL_DATABASE")
MYSQL_TABLE = os.getenv("FILESS_MYSQL_CITY_TABLE")

# PostgreSQL credentials
PG_HOST = "localhost"
PG_PORT = "5433"
PG_DATABASE = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Processing settings
DAYS_PER_BATCH = 250
DATE_COLUMN = "datetime"

# JDBC Jars
MYSQL_JAR = "/home/neosoft/Desktop/wheather_analytics_trends/infra/pyspark_apps/jars/mysql-connector-j-8.3.0.jar"
POSTGRES_JAR = "/home/neosoft/Desktop/wheather_analytics_trends/infra/pyspark_apps/jars/postgresql-42.7.3.jar"
