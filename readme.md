docker compose -f infra/docker-compose.yml build

docker compose -f infra/docker-compose.yml up

docker exec -it <airflow-container> airflow db init

docker compose -f infra/docker-compose.yml restart airflow-webserver airflow-scheduler
