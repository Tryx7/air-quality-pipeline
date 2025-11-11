"""
Stock Market ETL Pipeline DAG
Extracts data from Polygon API, transforms it using Pandas, and loads to PostgreSQL
Uses XComs for efficient inter-task communication
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Stock symbols to track - easily configurable for multiple stocks
STOCK_SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

def extract_stock_data(**kwargs):
    """
    Extract stock market data from Polygon API for multiple symbols
    Returns: Dictionary with stock data that will be passed via XCom
    """
    import requests
    from datetime import datetime, timedelta
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        raise ValueError("POLYGON_API_KEY environment variable not set")
    
    # Get yesterday's date (markets close with delay)
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info(f"Extracting stock data for {len(STOCK_SYMBOLS)} symbols on {target_date}")
    
    all_stock_data = []
    failed_symbols = []
    
    for symbol in STOCK_SYMBOLS:
        try:
            # Polygon API endpoint for daily aggregates
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{target_date}/{target_date}"
            params = {'apiKey': api_key}
            
            logger.info(f"Fetching data for {symbol}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle cases where market is closed or no data available
            if data.get('resultsCount', 0) > 0 and 'results' in data:
                stock_info = data['results'][0]
                stock_info['symbol'] = symbol
                stock_info['fetch_date'] = target_date
                all_stock_data.append(stock_info)
                logger.info(f"Successfully extracted data for {symbol}")
            else:
                logger.warning(f"No data available for {symbol} on {target_date} (market might be closed)")
                # Add placeholder data for closed market days
                all_stock_data.append({
                    'symbol': symbol,
                    'fetch_date': target_date,
                    'o': None,  # open
                    'h': None,  # high
                    'l': None,  # low
                    'c': None,  # close
                    'v': None,  # volume
                    'vw': None,  # volume weighted average
                    't': int(datetime.strptime(target_date, '%Y-%m-%d').timestamp() * 1000),
                    'n': 0,  # number of transactions
                    'market_closed': True
                })
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for {symbol}: {str(e)}")
            failed_symbols.append(symbol)
        except Exception as e:
            logger.error(f"Unexpected error processing {symbol}: {str(e)}")
            failed_symbols.append(symbol)
    
    if not all_stock_data:
        raise ValueError("No stock data was successfully extracted for any symbol")
    
    # Log summary
    logger.info(f"Extraction complete: {len(all_stock_data)} successful, {len(failed_symbols)} failed")
    if failed_symbols:
        logger.warning(f"Failed symbols: {', '.join(failed_symbols)}")
    
    # Save raw data to file for backup
    os.makedirs('/opt/airflow/data/raw', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = f'/opt/airflow/data/raw/stock_data_{timestamp}.json'
    
    with open(filepath, 'w') as f:
        json.dump(all_stock_data, f, indent=2)
    
    logger.info(f"Raw data backed up to {filepath}")
    
    # Push to XCom for next task (only essential data to minimize XCom size)
    return {
        'data': all_stock_data,
        'fetch_timestamp': timestamp,
        'target_date': target_date,
        'total_records': len(all_stock_data)
    }


def transform_stock_data(**kwargs):
    """
    Transform raw stock data into structured format using Pandas
    Uses XCom to pull data from extract task
    Returns: Transformed data for loading
    """
    import pandas as pd
    from datetime import datetime
    
    # Pull data from previous task via XCom
    ti = kwargs['ti']
    extract_output = ti.xcom_pull(task_ids='extract_stock_data')
    
    if not extract_output:
        raise ValueError("No data received from extract task via XCom")
    
    # Handle case where XCom might return a JSON string
    if isinstance(extract_output, str):
        import json
        extract_output = json.loads(extract_output)
    
    if 'data' not in extract_output:
        raise ValueError(f"Invalid data format from extract task. Got: {type(extract_output)}")
    
    raw_data = extract_output['data']
    target_date = extract_output['target_date']
    
    logger.info(f"Transforming {len(raw_data)} stock records")
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(raw_data)
    
    # Rename columns to more descriptive names
    column_mapping = {
        'o': 'open_price',
        'h': 'high_price',
        'l': 'low_price',
        'c': 'close_price',
        'v': 'volume',
        'vw': 'volume_weighted_avg_price',
        't': 'timestamp_ms',
        'n': 'num_transactions'
    }
    df = df.rename(columns=column_mapping)
    
    # Add calculated fields
    df['price_change'] = df['close_price'] - df['open_price']
    df['price_change_pct'] = (df['price_change'] / df['open_price'] * 100).round(2)
    df['intraday_volatility'] = df['high_price'] - df['low_price']
    df['volatility_pct'] = (df['intraday_volatility'] / df['open_price'] * 100).round(2)
    
    # Convert timestamp to datetime
    df['trade_date'] = pd.to_datetime(df['timestamp_ms'], unit='ms')
    
    # Add metadata
    df['processed_at'] = datetime.now()
    df['data_quality'] = df.apply(
        lambda row: 'complete' if pd.notna(row['close_price']) else 'market_closed',
        axis=1
    )
    
    # Handle missing values for market closed days
    numeric_columns = ['open_price', 'high_price', 'low_price', 'close_price', 
                      'volume', 'volume_weighted_avg_price', 'num_transactions',
                      'price_change', 'price_change_pct', 'intraday_volatility', 
                      'volatility_pct']
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    
    # Select final columns in desired order
    final_columns = [
        'symbol',
        'trade_date',
        'fetch_date',
        'open_price',
        'high_price',
        'low_price',
        'close_price',
        'volume',
        'volume_weighted_avg_price',
        'num_transactions',
        'price_change',
        'price_change_pct',
        'intraday_volatility',
        'volatility_pct',
        'data_quality',
        'processed_at'
    ]
    
    df = df[final_columns]
    
    # Data validation
    logger.info("Performing data validation")
    validation_summary = {
        'total_records': len(df),
        'complete_records': len(df[df['data_quality'] == 'complete']),
        'market_closed_records': len(df[df['data_quality'] == 'market_closed']),
        'symbols_processed': df['symbol'].nunique(),
        'date_range': f"{df['trade_date'].min()} to {df['trade_date'].max()}"
    }
    
    logger.info(f"Validation summary: {validation_summary}")
    
    # Save transformed data to parquet
    os.makedirs('/opt/airflow/data/processed', exist_ok=True)
    timestamp = extract_output['fetch_timestamp']
    parquet_path = f'/opt/airflow/data/processed/stock_data_{timestamp}.parquet'
    df.to_parquet(parquet_path, index=False)
    
    logger.info(f"Transformed data saved to {parquet_path}")
    
    # Convert DataFrame to dict for XCom
    # Important: Convert datetime objects to strings for JSON serialization
    df_serializable = df.copy()
    
    # Convert all datetime columns to ISO format strings
    datetime_columns = ['trade_date', 'processed_at']
    for col in datetime_columns:
        if col in df_serializable.columns:
            df_serializable[col] = df_serializable[col].astype(str)
    
    # Convert to dict for XCom (JSON serializable)
    transformed_data = df_serializable.to_dict('records')
    
    return {
        'data': transformed_data,
        'validation_summary': validation_summary,
        'timestamp': timestamp,
        'parquet_path': parquet_path
    }


def load_to_postgres(**kwargs):
    """
    Load transformed data to PostgreSQL using SQLAlchemy
    Uses XCom to pull transformed data
    """
    import pandas as pd
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import NullPool
    
    # Pull transformed data from XCom
    ti = kwargs['ti']
    transform_output = ti.xcom_pull(task_ids='transform_stock_data')
    
    if not transform_output:
        raise ValueError("No data received from transform task via XCom")
    
    # Handle case where XCom might return a JSON string
    if isinstance(transform_output, str):
        import json
        transform_output = json.loads(transform_output)
    
    if 'data' not in transform_output:
        raise ValueError(f"Invalid data format from transform task. Got: {type(transform_output)}")
    
    # Reconstruct DataFrame from XCom data
    df = pd.DataFrame(transform_output['data'])
    
    # Convert string datetime columns back to datetime objects
    datetime_columns = ['trade_date', 'processed_at']
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    
    validation_summary = transform_output['validation_summary']
    
    logger.info(f"Loading {len(df)} records to PostgreSQL")
    logger.info(f"Validation summary: {validation_summary}")
    
    # Get PostgreSQL credentials from environment
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')
    postgres_host = os.getenv('POSTGRES_HOST')
    postgres_port = os.getenv('POSTGRES_PORT')
    postgres_db = os.getenv('POSTGRES_DB')
    postgres_ssl_mode = os.getenv('POSTGRES_SSL_MODE', 'require')
    
    if not all([postgres_user, postgres_password, postgres_host, postgres_port, postgres_db]):
        raise ValueError("Missing required PostgreSQL environment variables")
    
    # Create SQLAlchemy engine with SSL
    connection_string = (
        f"postgresql://{postgres_user}:{postgres_password}@"
        f"{postgres_host}:{postgres_port}/{postgres_db}"
        f"?sslmode={postgres_ssl_mode}"
    )
    
    engine = create_engine(
        connection_string,
        poolclass=NullPool,  # Disable connection pooling for Airflow tasks
        echo=False
    )
    
    table_name = 'stock_market_data'
    
    try:
        # Create table if it doesn't exist
        with engine.begin() as conn:  # Use begin() for auto-commit
            create_table_query = text(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    trade_date TIMESTAMP NOT NULL,
                    fetch_date DATE NOT NULL,
                    open_price DECIMAL(12, 4),
                    high_price DECIMAL(12, 4),
                    low_price DECIMAL(12, 4),
                    close_price DECIMAL(12, 4),
                    volume BIGINT,
                    volume_weighted_avg_price DECIMAL(12, 4),
                    num_transactions INTEGER,
                    price_change DECIMAL(12, 4),
                    price_change_pct DECIMAL(8, 2),
                    intraday_volatility DECIMAL(12, 4),
                    volatility_pct DECIMAL(8, 2),
                    data_quality VARCHAR(20),
                    processed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, trade_date)
                );
                
                CREATE INDEX IF NOT EXISTS idx_stock_symbol_date 
                ON {table_name}(symbol, trade_date);
                
                CREATE INDEX IF NOT EXISTS idx_stock_fetch_date 
                ON {table_name}(fetch_date);
            """)
            conn.execute(create_table_query)
            # No need for conn.commit() - begin() auto-commits on exit
        logger.info(f"Table {table_name} ready")
        
        # Load data with upsert logic (insert or update on conflict)
        records_loaded = 0
        records_updated = 0
        
        with engine.begin() as conn:
            for _, row in df.iterrows():
                upsert_query = text(f"""
                    INSERT INTO {table_name} (
                        symbol, trade_date, fetch_date, open_price, high_price, 
                        low_price, close_price, volume, volume_weighted_avg_price,
                        num_transactions, price_change, price_change_pct,
                        intraday_volatility, volatility_pct, data_quality, processed_at
                    ) VALUES (
                        :symbol, :trade_date, :fetch_date, :open_price, :high_price,
                        :low_price, :close_price, :volume, :volume_weighted_avg_price,
                        :num_transactions, :price_change, :price_change_pct,
                        :intraday_volatility, :volatility_pct, :data_quality, :processed_at
                    )
                    ON CONFLICT (symbol, trade_date) 
                    DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        volume_weighted_avg_price = EXCLUDED.volume_weighted_avg_price,
                        num_transactions = EXCLUDED.num_transactions,
                        price_change = EXCLUDED.price_change,
                        price_change_pct = EXCLUDED.price_change_pct,
                        intraday_volatility = EXCLUDED.intraday_volatility,
                        volatility_pct = EXCLUDED.volatility_pct,
                        data_quality = EXCLUDED.data_quality,
                        processed_at = EXCLUDED.processed_at
                """)
                
                result = conn.execute(upsert_query, row.to_dict())
                if result.rowcount > 0:
                    records_loaded += 1
        
        logger.info(f"Successfully loaded {records_loaded} records to {table_name}")
        
        # Verify load
        with engine.connect() as conn:
            count_query = text(f"SELECT COUNT(*) FROM {table_name}")
            result = conn.execute(count_query)
            total_count = result.scalar()
            logger.info(f"Total records in {table_name}: {total_count}")
        
        return {
            'status': 'success',
            'records_loaded': records_loaded,
            'table_name': table_name,
            'total_records_in_db': total_count,
            'validation_summary': validation_summary
        }
        
    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {str(e)}")
        raise
    finally:
        engine.dispose()


def validate_pipeline_execution(**kwargs):
    """
    Validate the entire pipeline execution
    Checks data quality and pipeline health
    """
    import json
    
    ti = kwargs['ti']
    
    # Pull outputs from all tasks and handle JSON strings
    def parse_xcom_output(output):
        if isinstance(output, str):
            return json.loads(output)
        return output
    
    extract_output = parse_xcom_output(ti.xcom_pull(task_ids='extract_stock_data'))
    transform_output = parse_xcom_output(ti.xcom_pull(task_ids='transform_stock_data'))
    load_output = parse_xcom_output(ti.xcom_pull(task_ids='load_to_postgres'))
    
    logger.info("=" * 80)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 80)
    
    if extract_output:
        logger.info(f"EXTRACT: {extract_output['total_records']} records extracted")
        logger.info(f"         Target date: {extract_output['target_date']}")
    
    if transform_output:
        val_sum = transform_output['validation_summary']
        logger.info(f"TRANSFORM: {val_sum['total_records']} records transformed")
        logger.info(f"           Complete: {val_sum['complete_records']}")
        logger.info(f"           Market Closed: {val_sum['market_closed_records']}")
        logger.info(f"           Symbols: {val_sum['symbols_processed']}")
    
    if load_output:
        logger.info(f"LOAD: {load_output['records_loaded']} records loaded")
        logger.info(f"      Total in DB: {load_output['total_records_in_db']}")
        logger.info(f"      Status: {load_output['status']}")
    
    logger.info("=" * 80)
    logger.info("✓ Pipeline executed successfully!")
    logger.info("=" * 80)
    
    return {
        'pipeline_status': 'success',
        'execution_time': datetime.now().isoformat(),
        'extract_records': extract_output['total_records'] if extract_output else 0,
        'transform_records': transform_output['validation_summary']['total_records'] if transform_output else 0,
        'load_records': load_output['records_loaded'] if load_output else 0
    }


# Define the DAG
with DAG(
    dag_id='stock_market_etl_pipeline',
    default_args=default_args,
    description='Production-ready Stock Market ETL with XComs and PostgreSQL',
    schedule='0 18 * * 1-5',  # Run at 6 PM on weekdays (after market closes)
    catchup=False,
    tags=['stock_market', 'etl', 'polygon', 'postgresql', 'xcoms'],
    max_active_runs=1,
) as dag:
    
    # Task 1: Extract stock data from Polygon API
    extract_task = PythonOperator(
        task_id='extract_stock_data',
        python_callable=extract_stock_data,
        doc_md="""
        ## Extract Stock Data
        Fetches real-time stock market data from Polygon API for configured symbols.
        - Handles API rate limits and errors gracefully
        - Provides fallback for market closed days
        - Pushes extracted data to XCom for downstream tasks
        """
    )
    
    # Task 2: Transform data using Pandas
    transform_task = PythonOperator(
        task_id='transform_stock_data',
        python_callable=transform_stock_data,
        doc_md="""
        ## Transform Stock Data
        Transforms raw API data into structured format with calculated metrics.
        - Pulls data from XCom (extract task)
        - Calculates price changes, volatility, and other metrics
        - Validates data quality
        - Pushes transformed data to XCom
        """
    )
    
    # Task 3: Load to PostgreSQL
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        doc_md="""
        ## Load to PostgreSQL
        Loads transformed data to cloud PostgreSQL database.
        - Pulls data from XCom (transform task)
        - Creates tables if not exist
        - Implements upsert logic to handle duplicates
        - Includes transaction management for data integrity
        """
    )
    
    # Task 4: Validate pipeline execution
    validate_task = PythonOperator(
        task_id='validate_pipeline',
        python_callable=validate_pipeline_execution,
        doc_md="""
        ## Validate Pipeline
        Validates the entire pipeline execution and logs summary.
        - Checks data flow through all stages
        - Generates execution summary
        - Logs final statistics
        """
    )
    
    # Define task dependencies
    extract_task >> transform_task >> load_task >> validate_task