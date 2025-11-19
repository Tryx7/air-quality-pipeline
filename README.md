# Air Quality & Data Engineering Platform

A comprehensive data engineering platform featuring real-time air quality monitoring, stock market analytics, and YouTube data processing with Apache Airflow, Spark, Kafka, and multiple database technologies.

## 🏗️ Architecture Overview

```
Data Sources → Airflow ETL → Processing → Storage → Analytics
    ↓              ↓           ↓           ↓         ↓
 Air Quality     Spark       Kafka      PostgreSQL  Grafana
 Stock Market   PySpark     Cassandra   MongoDB
 YouTube API                Real-time
```

## 📁 Project Structure

```
├── dags/
│   ├── air_quality_pipeline.py     # Hourly air quality ETL
│   └── stock_market_dag.py         # Stock market ETL pipeline
├── scripts/
│   ├── spark_processing.py         # Spark data processing
│   └── air_quality_config.py       # Configuration files
├── docker-compose.yaml             # Multi-service infrastructure
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment template
└── data/                          # Data directories
    ├── raw/                       # Raw JSON data
    └── processed/                 # Processed Parquet files
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- API Keys for required services

### 1. Environment Setup

```bash
# Copy environment template
cp .env.example .env

# Edit with your credentials
nano .env
```

### 2. Required API Keys

Update `.env` with your credentials:

```env
# YouTube API
YOUTUBE_API_KEY=your_youtube_api_key_here
YOUTUBE_CHANNEL_ID=your_channel_id_here

# Polygon API (Stock Market)
POLYGON_API_KEY=your_polygon_api_key_here

# Aiven PostgreSQL
POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_db_password
POSTGRES_DB=your_database
POSTGRES_HOST=your_host.aivencloud.com
POSTGRES_PORT=12345
```

### 3. Start Infrastructure

```bash
# Build and start all services
docker compose up -d --build

# Check service status
docker compose ps
```

### 4. Initialize Airflow

```bash
# Access Airflow container
docker compose exec airflow-apiserver bash

# Create admin user (first time only)
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 5. Access Services

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Grafana**: http://localhost:3000 (admin/admin)
- **MongoDB**: localhost:27017
- **Kafka**: localhost:9092
- **Cassandra**: localhost:9042

## 🔧 Core Components

### 1. Air Quality Pipeline (`air_quality_pipeline.py`)

**Purpose**: Hourly monitoring of air quality in Kenyan cities

**Features**:
- Real-time data from Open-Meteo API for Nairobi and Mombasa
- AQI (Air Quality Index) calculations
- Health category classification
- Alert system for poor air quality
- Data storage in Aiven PostgreSQL

**Data Metrics**:
- PM2.5, PM10, Ozone, Carbon Monoxide
- Nitrogen Dioxide, Sulphur Dioxide, UV Index
- Calculated AQI and health categories

**Schedule**: Runs hourly

### 2. Stock Market ETL Pipeline (`stock_market_dag.py`)

**Purpose**: Daily extraction of stock market data from Polygon API

**Features**:
- Major stocks: AAPL, GOOGL, MSFT, AMZN, TSLA
- Financial data transformation and cleaning
- PostgreSQL storage with schema enforcement

**Data Schema**:
```sql
ticker | date | open_price | high_price | low_price | close_price | volume | vwap | transactions
```

**Schedule**: Runs daily at 2 AM

### 3. Spark Data Processing (`spark_processing.py`)

**Purpose**: Process YouTube analytics data using PySpark

**Features**:
- Channel statistics (subscribers, views, video count)
- Video analytics with engagement rates
- Publishing pattern analysis
- Output to PostgreSQL and Parquet

**Output Tables**:
- `channel_stats`: Channel-level metrics
- `video_stats`: Video-level analytics

### 4. Data Infrastructure Services

**Databases**:
- **PostgreSQL**: Primary relational store (Aiven)
- **MongoDB**: Document storage for unstructured data
- **Cassandra**: Time-series and high-write workloads

**Streaming & Messaging**:
- **Kafka**: Real-time data streaming
- **Zookeeper**: Kafka coordination

**Monitoring**:
- **Grafana**: Data visualization and dashboards
- **Airflow**: Workflow orchestration

## ⚙️ Configuration

### Environment Variables

Create `.env` file with required credentials:

```env
# Airflow
AIRFLOW_UID=1000
AIRFLOW_IMAGE_NAME=custom-airflow:pyspark

# YouTube API
YOUTUBE_API_KEY=your_actual_key
YOUTUBE_CHANNEL_ID=your_actual_channel

# Polygon API
POLYGON_API_KEY=your_actual_key

# Aiven PostgreSQL
POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_db_password
POSTGRES_DB=your_database
POSTGRES_HOST=your_host.aivencloud.com
POSTGRES_PORT=12345
POSTGRES_SSL_MODE=require
```

### Air Quality Configuration

The air quality pipeline monitors:
- **Nairobi**: Latitude -1.286389, Longitude 36.817223
- **Mombasa**: Latitude -4.043477, Longitude 39.668206

## 📊 Data Pipelines

### Air Quality Pipeline Flow
```
Open-Meteo API → Data Extraction → AQI Calculation → PostgreSQL → Analytics
      ↓               ↓               ↓               ↓           ↓
  Real-time        Validation    Health Alerts    Storage    Grafana Dashboards
```

### Stock Market Pipeline Flow
```
Polygon API → Data Extraction → Transformation → PostgreSQL → Analytics
     ↓              ↓               ↓              ↓           ↓
 Daily Data      Validation     Price Cleaning   Warehouse  Performance Metrics
```

### YouTube Analytics Flow
```
YouTube API → Spark Processing → PostgreSQL → Analytics
     ↓             ↓               ↓           ↓
 Channel Data  Engagement Rates  Storage    View Patterns
```

## 🛠️ Development

### Running Spark Jobs

```bash
# Submit Spark job manually
docker compose exec airflow-apiserver python /opt/airflow/scripts/spark_processing.py
```

### Manual DAG Execution

```bash
# Trigger air quality pipeline
docker compose exec airflow-apiserver airflow dags trigger air_quality_pipeline

# Trigger stock market pipeline
docker compose exec airflow-apiserver airflow dags trigger stock_market_etl_pipeline
```

### Database Connections

```python
# PostgreSQL Connection
conn_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?sslmode=require"

# MongoDB Connection
from pymongo import MongoClient
client = MongoClient('mongodb://admin:password@mongodb:27017/')
```

## 📈 Monitoring & Operations

### Airflow Dashboard
- Monitor DAG execution and task status
- View execution logs and retry failed tasks
- Manage variables and connections

### Grafana Analytics
- Create dashboards for air quality trends
- Monitor stock performance metrics
- Visualize YouTube channel analytics

### Service Health Checks

```bash
# Check all services
docker compose ps

# View specific service logs
docker compose logs airflow-scheduler
docker compose logs kafka

# Check database connectivity
docker compose exec postgres psql -U airflow -d airflow
```

## 🔄 Pipeline Details

### Air Quality AQI Categories
- **0-50**: Good
- **51-100**: Moderate  
- **101-150**: Unhealthy for Sensitive Groups
- **151-200**: Unhealthy
- **201-300**: Very Unhealthy
- **301+**: Hazardous

### Stock Market Coverage
- Apple (AAPL), Google (GOOGL), Microsoft (MSFT)
- Amazon (AMZN), Tesla (TSLA)

### YouTube Analytics
- Channel subscriber growth
- Video engagement rates
- Publishing time analysis
- Content performance metrics

## 🐛 Troubleshooting

### Common Issues

**Airflow DAG not appearing**:
- Check DAG files are in `dags/` directory
- Verify no syntax errors in Python files
- Restart airflow-scheduler service

**Database connection failures**:
- Verify credentials in `.env` file
- Check network connectivity to Aiven
- Confirm SSL certificates are trusted

**API Key Issues**:
- Verify YouTube Data API v3 is enabled
- Check Polygon.io subscription status
- Confirm API rate limits aren't exceeded

### Logs and Debugging

```bash
# View specific service logs
docker compose logs airflow-scheduler
docker compose logs kafka

# Debug DAG execution
docker compose exec airflow-apiserver airflow tasks list air_quality_pipeline

# Check database content
docker compose exec airflow-apiserver python -c "
from sqlalchemy import create_engine, text
import os
engine = create_engine(f'postgresql://{os.getenv(\"POSTGRES_USER\")}:{os.getenv(\"POSTGRES_PASSWORD\")}@{os.getenv(\"POSTGRES_HOST\")}:{os.getenv(\"POSTGRES_PORT\")}/{os.getenv(\"POSTGRES_DB\")}?sslmode=require')
with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM air_quality_readings'))
    print(f'Records: {result.scalar()}')
"
```

### Reset Environment

```bash
# Stop and remove containers
docker compose down

# Clean volumes (removes all data)
docker compose down -v

# Restart fresh
docker compose up -d
```

## 📝 License

This project is for educational and demonstration purposes. Adapt and extend for your specific use cases.

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/improvement`)
5. Create Pull Request

---

**Note**: Replace all placeholder credentials in `.env` with your actual API keys and database credentials before running the project. Ensure you have proper subscriptions for Polygon.io and YouTube Data API v3.
