from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_youtube_data(**kwargs):
    """Extract data from YouTube API"""
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    from youtube_ingestion import fetch_channel_data, fetch_videos_data, save_data
    from datetime import datetime
    
    api_key = os.getenv('YOUTUBE_API_KEY')
    channel_id = os.getenv('YOUTUBE_CHANNEL_ID')
    
    channel_data = fetch_channel_data(api_key, channel_id)
    videos_data = fetch_videos_data(api_key, channel_id, max_results=100)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    save_data(channel_data, f'channel_data_{timestamp}.json')
    save_data(videos_data, f'videos_data_{timestamp}.json')
    
    return timestamp

def transform_data(**kwargs):
    """Transform data using PySpark"""
    import subprocess
    result = subprocess.run(
        ['python', '/opt/airflow/scripts/spark_processing.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Spark job failed: {result.stderr}")
    return result.stdout

with DAG(
    'youtube_analytics_pipeline',
    default_args=default_args,
    description='End-to-end YouTube analytics pipeline',
    schedule='@daily',
    catchup=False,
    tags=['youtube', 'analytics'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_youtube_data',
        python_callable=extract_youtube_data,
    )
    
    transform_task = PythonOperator(
        task_id='transform_with_pyspark',
        python_callable=transform_data,
    )
    
    validate_task = BashOperator(
        task_id='validate_data',
        bash_command='echo "Data validation completed"',
    )
    
    extract_task >> transform_task >> validate_task