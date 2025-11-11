# scripts/air_quality_config.py
"""
Configuration for Air Quality Pipeline
"""

# API Configuration
OPEN_METEO_BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['kafka:9092'],
    'topic': 'air-quality-readings'
}

# MongoDB Configuration
MONGO_CONFIG = {
    'connection_string': 'mongodb://admin:password@mongodb:27017/',
    'database': 'air_quality_db',
    'collection': 'hourly_readings'
}

# Cassandra Configuration
CASSANDRA_CONFIG = {
    'hosts': ['cassandra'],
    'port': 9042,
    'keyspace': 'air_quality',
    'table': 'hourly_air_quality'
}