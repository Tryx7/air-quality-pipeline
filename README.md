# Build the Pipeline
docker compose build

# Start pipeline
docker compose up -d

# Stop pipeline
docker compose down

# View logs
docker compose logs -f

# Restart Airflow
docker compose restart airflow-webserver airflow-scheduler


# Wait 5-10 minutes for PySpark to download (it's 434 MB). You can monitor:

docker compose logs -f airflow-scheduler | grep pyspark# dockerised-kafka-pyspark-grafana
