API key => 02e8f21eae05e093481569b87fdf3865

docker compose -f infra/docker-compose.yml build

docker compose -f infra/docker-compose.yml up

docker compose -f infra/docker-compose.yml down -v


docker exec -it <airflow-container> airflow db init

docker compose -f infra/docker-compose.yml restart airflow-webserver airflow-scheduler

spark-submit \
  --jars /infra/pyspark_apps/jars/mysql-connector-j-8.3.0.jar,/infra/pyspark_apps/jars/spark-excel_2.12-0.13.7.jar \
  ingest_sql.py

docker exec -i mysql_weather mysql -u root -pweather_password < mysql_init/init.sql

docker exec -i postgres_weather psql -U weather_user -d weather_analytics < postgres_init/init_dw.sql

spark-submit \
    --jars /infra/pyspark_apps/jars/mysql-connector-j-8.3.0.jar,/infra/pyspark_apps/jars/spark-excel_2.12-0.13.7.jar \
    ingest_sql.py


###
mysql \
  -h f8hi32.h.filess.io \
  -P 3307 \
  -u RawWeatherDB_variousarm \
  -p962785a84f559db7e6ad3d5f17ba8418566b2304 \
  RawWeatherDB_variousarm -e "SHOW TABLES;"


spark-submit \
  --jars /infra/pyspark_apps/jars/mysql-connector-j-8.3.0.jar,/infra/pyspark_apps/jars/spark-excel_2.12-0.13.7.jar \
  ingest_sql.py

spark-submit \
  --jars infra/pyspark_apps/jars/mysql-connector-j-8.3.0.jar,infra/pyspark_apps/jars/spark-excel_2.12-0.13.7.jar \
  scripts/ingest_sql.py

spark-submit \
  --jars /home/neosoft/Desktop/wheather_analytics_trends/infra/pyspark_apps/jars/mysql-connector-j-8.3.0.jar,/home/neosoft/Desktop/wheather_analytics_trends/infra/pyspark_apps/jars/spark-excel_2.12-0.13.7.jar \
  /home/neosoft/Desktop/wheather_analytics_trends/scripts/ingest_sql.py

spark-submit \
  --jars \
/home/neosoft/Desktop/wheather_analytics_trends/infra/pyspark_apps/jars/mysql-connector-j-8.3.0.jar,\
/home/neosoft/Desktop/wheather_analytics_trends/infra/pyspark_apps/jars/spark-excel_2.12-0.13.7.jar,\
/home/neosoft/Desktop/wheather_analytics_trends/infra/pyspark_apps/jars/poi-ooxml-5.2.5.jar,\
/home/neosoft/Desktop/wheather_analytics_trends/infra/pyspark_apps/jars/poi-ooxml-schemas-4.1.2.jar,\
/home/neosoft/Desktop/wheather_analytics_trends/infra/pyspark_apps/jars/xmlbeans-5.1.1.jar,\
/home/neosoft/Desktop/wheather_analytics_trends/infra/pyspark_apps/jars/commons-collections4-4.4.jar \
  /home/neosoft/Desktop/wheather_analytics_trends/scripts/ingest_sql.py

spark-submit --jars /home/neosoft/Desktop/wheather_analytics_trends/infra/pyspark_apps/jars/*.jar /home/neosoft/Desktop/wheather_analytics_trends/scripts/ingest_sql.py

mysql -u  -p

mysql -h f8hi32.h.filess.io -P 3307 -u RawWeatherDB_variousarm -p

2024-08-01

spark-submit \
  --jars infra/pyspark_apps/jars/mysql-connector-j-8.3.0.jar,infra/pyspark_apps/jars/postgresql-42.7.3.jar \
  pyspark_jobs/process_weather_data.py


docker compose --env-file .env -f infra/docker-compose.yml up


postgresql://weather_user:weather_password@postgres_weather:5432/weather_analytics