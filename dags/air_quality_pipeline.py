# dags/air_quality_pipeline.py
"""
Air Quality API Pipeline
Ingests hourly air quality measurements from Open-Meteo API for Nairobi and Mombasa
Stores in Aiven PostgreSQL for analytics
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import logging

# Configure logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# City coordinates
CITIES = {
    'nairobi': {
        'latitude': -1.286389,
        'longitude': 36.817223,
        'timezone': 'Africa/Nairobi'
    },
    'mombasa': {
        'latitude': -4.043477, 
        'longitude': 39.668206,
        'timezone': 'Africa/Nairobi'
    }
}

def fetch_air_quality_data(**kwargs):
    """
    Fetch air quality data from Open-Meteo API for both cities
    """
    import requests
    from datetime import datetime
    
    all_city_data = []
    
    for city_name, coords in CITIES.items():
        try:
            # Construct API URL
            base_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
            params = {
                'latitude': coords['latitude'],
                'longitude': coords['longitude'],
                'hourly': 'pm2_5,pm10,ozone,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,uv_index',
                'timezone': coords['timezone'],
                'forecast_days': 1
            }
            
            logger.info(f"Fetching air quality data for {city_name}")
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Process the hourly data - get only the latest reading
            if 'hourly' in data and 'time' in data['hourly']:
                times = data['hourly']['time']
                latest_index = len(times) - 1  # Get the most recent reading
                
                air_quality_reading = {
                    'city': city_name,
                    'latitude': coords['latitude'],
                    'longitude': coords['longitude'],
                    'timestamp': times[latest_index],
                    'fetch_time': datetime.now().isoformat(),
                    'pm25': data['hourly']['pm2_5'][latest_index] if data['hourly']['pm2_5'][latest_index] is not None else 0,
                    'pm10': data['hourly']['pm10'][latest_index] if data['hourly']['pm10'][latest_index] is not None else 0,
                    'ozone': data['hourly']['ozone'][latest_index] if data['hourly']['ozone'][latest_index] is not None else 0,
                    'carbon_monoxide': data['hourly']['carbon_monoxide'][latest_index] if data['hourly']['carbon_monoxide'][latest_index] is not None else 0,
                    'nitrogen_dioxide': data['hourly']['nitrogen_dioxide'][latest_index] if data['hourly']['nitrogen_dioxide'][latest_index] is not None else 0,
                    'sulphur_dioxide': data['hourly']['sulphur_dioxide'][latest_index] if data['hourly']['sulphur_dioxide'][latest_index] is not None else 0,
                    'uv_index': data['hourly']['uv_index'][latest_index] if data['hourly']['uv_index'][latest_index] is not None else 0,
                }
                
                # Calculate AQI
                air_quality_reading['aqi_pm25'] = calculate_aqi_pm25(air_quality_reading['pm25'])
                air_quality_reading['aqi_pm10'] = calculate_aqi_pm10(air_quality_reading['pm10'])
                air_quality_reading['aqi_overall'] = max(air_quality_reading['aqi_pm25'], air_quality_reading['aqi_pm10'])
                air_quality_reading['health_category'] = get_health_category(air_quality_reading['aqi_overall'])
                
                all_city_data.append(air_quality_reading)
                logger.info(f"Successfully extracted data for {city_name}: AQI {air_quality_reading['aqi_overall']:.1f}")
            
        except Exception as e:
            logger.error(f"Failed to fetch data for {city_name}: {str(e)}")
            # Continue with other cities even if one fails
    
    if not all_city_data:
        raise ValueError("No air quality data was successfully extracted for any city")
    
    # Save raw data to file for backup
    os.makedirs('/opt/airflow/data/raw/air_quality', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = f'/opt/airflow/data/raw/air_quality/air_quality_{timestamp}.json'
    
    with open(filepath, 'w') as f:
        json.dump(all_city_data, f, indent=2)
    
    logger.info(f"Raw air quality data backed up to {filepath}")
    
    return {
        'data': all_city_data,
        'fetch_timestamp': timestamp,
        'total_readings': len(all_city_data)
    }

def calculate_aqi_pm25(pm25):
    """Calculate AQI for PM2.5"""
    if pm25 <= 12.0:
        return (pm25 / 12.0) * 50
    elif pm25 <= 35.4:
        return 51 + ((pm25 - 12.1) / (35.4 - 12.1)) * 49
    elif pm25 <= 55.4:
        return 101 + ((pm25 - 35.5) / (55.4 - 35.5)) * 49
    elif pm25 <= 150.4:
        return 151 + ((pm25 - 55.5) / (150.4 - 55.5)) * 49
    else:
        return 201

def calculate_aqi_pm10(pm10):
    """Calculate AQI for PM10"""
    if pm10 <= 54:
        return (pm10 / 54) * 50
    elif pm10 <= 154:
        return 51 + ((pm10 - 55) / (154 - 55)) * 49
    elif pm10 <= 254:
        return 101 + ((pm10 - 155) / (254 - 155)) * 49
    else:
        return 151

def get_health_category(aqi):
    """Get health category based on AQI"""
    if aqi <= 50:
        return "Good"
    elif aqi <= 100:
        return "Moderate"
    elif aqi <= 150:
        return "Unhealthy for Sensitive Groups"
    elif aqi <= 200:
        return "Unhealthy"
    elif aqi <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

def store_in_postgres(**kwargs):
    """
    Store air quality data in Aiven PostgreSQL
    """
    from sqlalchemy import create_engine, text
    
    # Pull data from previous task
    ti = kwargs['ti']
    fetch_output = ti.xcom_pull(task_ids='fetch_air_quality_data')
    
    if not fetch_output:
        raise ValueError("No data received from fetch task")
    
    if isinstance(fetch_output, str):
        fetch_output = json.loads(fetch_output)
    
    air_quality_data = fetch_output['data']
    
    logger.info(f"Storing {len(air_quality_data)} air quality readings in Aiven PostgreSQL")
    
    # Get Aiven PostgreSQL credentials from environment
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')
    postgres_host = os.getenv('POSTGRES_HOST')
    postgres_port = os.getenv('POSTGRES_PORT')
    postgres_db = os.getenv('POSTGRES_DB')
    postgres_ssl_mode = os.getenv('POSTGRES_SSL_MODE', 'require')
    
    if not all([postgres_user, postgres_password, postgres_host, postgres_port, postgres_db]):
        raise ValueError("Missing required PostgreSQL environment variables")
    
    try:
        # Create SQLAlchemy engine with SSL
        connection_string = (
            f"postgresql://{postgres_user}:{postgres_password}@"
            f"{postgres_host}:{postgres_port}/{postgres_db}"
        )
        
        engine = create_engine(
            connection_string,
            connect_args={
                'sslmode': postgres_ssl_mode,
            },
            echo=False
        )
        
        # Create table if it doesn't exist
        with engine.begin() as conn:
            create_table_query = text("""
                CREATE TABLE IF NOT EXISTS air_quality_readings (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(50) NOT NULL,
                    latitude DECIMAL(10, 6),
                    longitude DECIMAL(10, 6),
                    timestamp TIMESTAMP NOT NULL,
                    fetch_time TIMESTAMP NOT NULL,
                    pm25 DECIMAL(8, 2),
                    pm10 DECIMAL(8, 2),
                    ozone DECIMAL(8, 2),
                    carbon_monoxide DECIMAL(8, 2),
                    nitrogen_dioxide DECIMAL(8, 2),
                    sulphur_dioxide DECIMAL(8, 2),
                    uv_index DECIMAL(8, 2),
                    aqi_pm25 DECIMAL(5, 2),
                    aqi_pm10 DECIMAL(5, 2),
                    aqi_overall DECIMAL(5, 2),
                    health_category VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(city, timestamp)
                )
            """)
            conn.execute(create_table_query)
            
            # Create index for performance
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_air_quality_city_timestamp 
                ON air_quality_readings(city, timestamp DESC)
            """))
        
        # Insert data
        records_loaded = 0
        for reading in air_quality_data:
            try:
                with engine.begin() as conn:
                    upsert_query = text("""
                        INSERT INTO air_quality_readings (
                            city, latitude, longitude, timestamp, fetch_time,
                            pm25, pm10, ozone, carbon_monoxide, nitrogen_dioxide, 
                            sulphur_dioxide, uv_index, aqi_pm25, aqi_pm10, aqi_overall, health_category
                        ) VALUES (
                            :city, :latitude, :longitude, :timestamp, :fetch_time,
                            :pm25, :pm10, :ozone, :carbon_monoxide, :nitrogen_dioxide, 
                            :sulphur_dioxide, :uv_index, :aqi_pm25, :aqi_pm10, :aqi_overall, :health_category
                        )
                        ON CONFLICT (city, timestamp) 
                        DO UPDATE SET
                            pm25 = EXCLUDED.pm25,
                            pm10 = EXCLUDED.pm10,
                            ozone = EXCLUDED.ozone,
                            carbon_monoxide = EXCLUDED.carbon_monoxide,
                            nitrogen_dioxide = EXCLUDED.nitrogen_dioxide,
                            sulphur_dioxide = EXCLUDED.sulphur_dioxide,
                            uv_index = EXCLUDED.uv_index,
                            aqi_pm25 = EXCLUDED.aqi_pm25,
                            aqi_pm10 = EXCLUDED.aqi_pm10,
                            aqi_overall = EXCLUDED.aqi_overall,
                            health_category = EXCLUDED.health_category,
                            fetch_time = EXCLUDED.fetch_time
                    """)
                    
                    conn.execute(upsert_query, {
                        'city': reading['city'],
                        'latitude': reading['latitude'],
                        'longitude': reading['longitude'],
                        'timestamp': reading['timestamp'],
                        'fetch_time': reading['fetch_time'],
                        'pm25': reading['pm25'],
                        'pm10': reading['pm10'],
                        'ozone': reading['ozone'],
                        'carbon_monoxide': reading['carbon_monoxide'],
                        'nitrogen_dioxide': reading['nitrogen_dioxide'],
                        'sulphur_dioxide': reading['sulphur_dioxide'],
                        'uv_index': reading['uv_index'],
                        'aqi_pm25': reading['aqi_pm25'],
                        'aqi_pm10': reading['aqi_pm10'],
                        'aqi_overall': reading['aqi_overall'],
                        'health_category': reading['health_category']
                    })
                    
                    records_loaded += 1
                    
            except Exception as e:
                logger.error(f"Error inserting record for {reading['city']}: {str(e)}")
        
        logger.info(f"Successfully loaded {records_loaded} records to PostgreSQL")
        
        return {
            'status': 'success',
            'records_loaded': records_loaded,
            'table': 'air_quality_readings'
        }
        
    except Exception as e:
        logger.error(f"Error storing data in PostgreSQL: {str(e)}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()

def generate_analytics(**kwargs):
    """
    Generate simple analytics for air quality data
    """
    from sqlalchemy import create_engine, text
    import pandas as pd
    
    # Get Aiven PostgreSQL credentials
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')
    postgres_host = os.getenv('POSTGRES_HOST')
    postgres_port = os.getenv('POSTGRES_PORT')
    postgres_db = os.getenv('POSTGRES_DB')
    postgres_ssl_mode = os.getenv('POSTGRES_SSL_MODE', 'require')
    
    try:
        connection_string = (
            f"postgresql://{postgres_user}:{postgres_password}@"
            f"{postgres_host}:{postgres_port}/{postgres_db}"
        )
        
        engine = create_engine(
            connection_string,
            connect_args={'sslmode': postgres_ssl_mode},
            echo=False
        )
        
        # Generate simple analytics
        with engine.connect() as conn:
            # Current air quality status
            current_aq = pd.read_sql("""
                SELECT city, aqi_overall, health_category, timestamp
                FROM air_quality_readings 
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
                ORDER BY timestamp DESC, city
            """, conn)
            
            # Alert conditions
            alerts = pd.read_sql("""
                SELECT city, timestamp, aqi_overall, health_category
                FROM air_quality_readings 
                WHERE aqi_overall > 100 
                AND timestamp >= NOW() - INTERVAL '1 hour'
                ORDER BY aqi_overall DESC
            """, conn)
        
        analytics_summary = {
            'current_readings': len(current_aq),
            'cities_with_poor_air_quality': len(alerts),
            'alerts_triggered': len(alerts),
            'worst_aqi': current_aq['aqi_overall'].max() if not current_aq.empty else 0,
            'best_aqi': current_aq['aqi_overall'].min() if not current_aq.empty else 0
        }
        
        logger.info("Air Quality Analytics Summary:")
        logger.info(f"  - Current readings: {analytics_summary['current_readings']}")
        logger.info(f"  - Cities with poor air quality: {analytics_summary['cities_with_poor_air_quality']}")
        logger.info(f"  - Worst AQI: {analytics_summary['worst_aqi']:.1f}")
        logger.info(f"  - Best AQI: {analytics_summary['best_aqi']:.1f}")
        
        # Log alerts
        if not alerts.empty:
            logger.warning("AIR QUALITY ALERTS:")
            for _, alert in alerts.iterrows():
                logger.warning(f"  - {alert['city']}: AQI {alert['aqi_overall']:.1f} ({alert['health_category']})")
        else:
            logger.info("  - No air quality alerts triggered")
        
        return analytics_summary
        
    except Exception as e:
        logger.error(f"Error generating analytics: {str(e)}")
        # Don't raise exception - analytics failure shouldn't fail the whole pipeline
        return {
            'current_readings': 0,
            'cities_with_poor_air_quality': 0,
            'alerts_triggered': 0,
            'worst_aqi': 0,
            'best_aqi': 0,
            'error': str(e)
        }
    finally:
        if 'engine' in locals():
            engine.dispose()

def validate_pipeline(**kwargs):
    """
    Validate the pipeline execution
    """
    ti = kwargs['ti']
    
    fetch_output = ti.xcom_pull(task_ids='fetch_air_quality_data')
    postgres_output = ti.xcom_pull(task_ids='store_in_postgres')
    analytics_output = ti.xcom_pull(task_ids='generate_analytics')
    
    if isinstance(fetch_output, str):
        fetch_output = json.loads(fetch_output)
    if isinstance(postgres_output, str):
        postgres_output = json.loads(postgres_output)
    if isinstance(analytics_output, str):
        analytics_output = json.loads(analytics_output)
    
    logger.info("=" * 60)
    logger.info("AIR QUALITY PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    
    if fetch_output:
        logger.info(f"✓ Fetched {fetch_output['total_readings']} air quality readings")
        for reading in fetch_output['data']:
            logger.info(f"  - {reading['city']}: AQI {reading['aqi_overall']:.1f} ({reading['health_category']})")
    
    if postgres_output:
        logger.info(f"✓ Stored {postgres_output['records_loaded']} records in PostgreSQL")
    
    if analytics_output:
        logger.info(f"✓ Analytics: {analytics_output['current_readings']} current readings")
        logger.info(f"  Worst AQI: {analytics_output['worst_aqi']:.1f}")
        logger.info(f"  Alerts: {analytics_output['alerts_triggered']}")
    
    logger.info("=" * 60)
    logger.info("Pipeline completed successfully!")
    logger.info("=" * 60)
    
    return {
        'pipeline_status': 'success',
        'execution_time': datetime.now().isoformat()
    }

# Define the DAG
with DAG(
    dag_id='air_quality_pipeline',
    default_args=default_args,
    description='Hourly Air Quality Data Pipeline for Kenyan Cities',
    schedule='0 * * * *',  # Run hourly
    catchup=False,
    tags=['air-quality', 'api', 'postgresql', 'kenya'],
    max_active_runs=1,
) as dag:
    
    # Task 1: Fetch air quality data from API
    fetch_task = PythonOperator(
        task_id='fetch_air_quality_data',
        python_callable=fetch_air_quality_data,
        doc_md="Fetch air quality data from Open-Meteo API for Nairobi and Mombasa"
    )
    
    # Task 2: Store in PostgreSQL
    postgres_task = PythonOperator(
        task_id='store_in_postgres',
        python_callable=store_in_postgres,
        doc_md="Store air quality data in Aiven PostgreSQL with AQI calculations"
    )
    
    # Task 3: Generate Analytics
    analytics_task = PythonOperator(
        task_id='generate_analytics',
        python_callable=generate_analytics,
        doc_md="Generate air quality analytics and alerts"
    )
    
    # Task 4: Validate pipeline
    validate_task = PythonOperator(
        task_id='validate_pipeline',
        python_callable=validate_pipeline,
        doc_md="Validate pipeline execution and log summary"
    )
    
    # Define task dependencies
    fetch_task >> postgres_task >> analytics_task >> validate_task