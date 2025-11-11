These files work together to create a **YouTube Analytics Pipeline** using Apache Airflow for orchestration, PySpark for data processing, and PostgreSQL for storage. Here's how they interact:

## File Interactions

### 1. **youtube_analytics_dag.py** (Orchestrator)
- **Role**: Airflow DAG that coordinates the entire workflow
- **Triggers**: `youtube_ingestion.py` → `spark_processing.py`
- **Manages**: Task dependencies and scheduling

### 2. **youtube_ingestion.py** (Data Extraction)
- **Role**: Fetches raw data from YouTube API
- **Output**: JSON files in `data/raw/` directory
- **Called by**: Airflow's `extract_youtube_data` task

### 3. **spark_processing.py** (Data Transformation & Loading)
- **Role**: Processes raw JSON data and loads to PostgreSQL
- **Input**: JSON files from `youtube_ingestion.py`
- **Output**: PostgreSQL tables + Parquet files

## Step-by-Step Integration Process

### Step 1: Project Structure Setup
```
your_project/
├── dags/
│   └── youtube_analytics_dag.py
├── scripts/
│   ├── youtube_ingestion.py
│   └── spark_processing.py
├── data/
│   ├── raw/          (created automatically)
│   └── processed/    (created automatically)
├── docker-compose.yml
└── requirements.txt
```

### Step 2: Create Docker Compose File
```yaml
# docker-compose.yml
version: '3'
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"

  airflow:
    image: apache/airflow:2.7.1
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'
      YOUTUBE_API_KEY: ${YOUTUBE_API_KEY}
      YOUTUBE_CHANNEL_ID: ${YOUTUBE_CHANNEL_ID}
    volumes:
      - ./dags:/opt/airflow/dags
      - ./scripts:/opt/airflow/scripts
      - ./data:/opt/airflow/data
    ports:
      - "8080:8080"
    depends_on:
      - postgres
    command: >
      bash -c "
        airflow db init &&
        airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com &&
        airflow webserver & airflow scheduler
      "
```

### Step 3: Create Requirements File
```txt
# requirements.txt
apache-airflow==2.7.1
pyspark==3.4.0
google-api-python-client==2.86.0
psycopg2-binary==2.9.6
```

### Step 4: Environment Variables Setup
Create a `.env` file:
```env
YOUTUBE_API_KEY=your_youtube_api_key_here
YOUTUBE_CHANNEL_ID=your_channel_id_here
```

### Step 5: Fix File Paths in Your Code

**Update `spark_processing.py`:**
```python
# Change this line in the main block:
channel_file = sorted(glob.glob('/opt/airflow/data/raw/channel_data_*.json'))[-1]
videos_file = sorted(glob.glob('/opt/airflow/data/raw/videos_data_*.json'))[-1]

# And these lines:
channel_df.write.mode('overwrite').parquet('/opt/airflow/data/processed/channel_stats.parquet')
videos_df.write.mode('overwrite').parquet('/opt/airflow/data/processed/video_stats.parquet')
```

**Update `youtube_ingestion.py`:**
```python
def save_data(data, filename):
    """Save data to JSON file"""
    os.makedirs('/opt/airflow/data/raw', exist_ok=True)
    filepath = f'/opt/airflow/data/raw/{filename}'
    # ... rest of the function
```

### Step 6: Build and Run
```bash
# 1. Set environment variables
export YOUTUBE_API_KEY=your_actual_api_key
export YOUTUBE_CHANNEL_ID=your_actual_channel_id

# 2. Start the stack
docker-compose up -d

# 3. Check if services are running
docker-compose ps

# 4. Access Airflow UI at http://localhost:8080
#    Login: admin/admin
```

### Step 7: Test the Pipeline
1. **Access Airflow UI** at `http://localhost:8080`
2. **Enable the DAG**: Find `youtube_analytics_pipeline` and toggle it on
3. **Trigger manually**: Click the play button to run immediately
4. **Monitor progress**: Watch the task execution in Airflow

## Data Flow Sequence

1. **Extract** (Airflow → youtube_ingestion.py)
   - Fetches channel stats and videos from YouTube API
   - Saves as timestamped JSON files in `data/raw/`

2. **Transform** (Airflow → spark_processing.py)
   - PySpark reads the latest JSON files
   - Cleans, enriches, and calculates engagement metrics
   - Creates structured DataFrames

3. **Load** (spark_processing.py → PostgreSQL)
   - Writes processed data to PostgreSQL tables
   - Also saves as Parquet files for backup

4. **Validate** (Airflow)
   - Placeholder for data quality checks

## Key Dependencies
- **Airflow**: Orchestration and scheduling
- **PySpark**: Distributed data processing
- **PostgreSQL**: Data storage and analytics
- **YouTube API**: Data source
- **Docker**: Containerization and environment management

This creates a complete, automated pipeline that runs daily to extract YouTube analytics data, process it, and load it into your database for analysis.