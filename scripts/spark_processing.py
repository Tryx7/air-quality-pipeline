from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import glob
from datetime import datetime
import os

def create_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("YouTubeAnalytics") \
        .config("spark.jars", "/home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/postgresql-42.7.1.jar") \
        .getOrCreate()

def process_channel_data(spark, input_path):
    """Process channel data"""
    with open(input_path, 'r') as f:
        data = json.load(f)
    
    channel_df = spark.createDataFrame([{
        'channel_id': data['id'],
        'channel_name': data['snippet']['title'],
        'subscribers': int(data['statistics']['subscriberCount']),
        'total_views': int(data['statistics']['viewCount']),
        'video_count': int(data['statistics']['videoCount']),
        'fetch_date': datetime.now()
    }])
    
    return channel_df

def process_videos_data(spark, input_path):
    """Process videos data"""
    with open(input_path, 'r') as f:
        data = json.load(f)
    
    videos_list = []
    for video in data:
        videos_list.append({
            'video_id': video['id'],
            'title': video['snippet']['title'],
            'published_at': video['snippet']['publishedAt'],
            'views': int(video['statistics'].get('viewCount', 0)),
            'likes': int(video['statistics'].get('likeCount', 0)),
            'comments': int(video['statistics'].get('commentCount', 0)),
            'duration': video['contentDetails']['duration']
        })
    
    videos_df = spark.createDataFrame(videos_list)
    
    # Transform data
    videos_df = videos_df.withColumn(
        'published_datetime',
        to_timestamp('published_at')
    ).withColumn(
        'publish_hour',
        hour('published_datetime')
    ).withColumn(
        'publish_day',
        dayofweek('published_datetime')
    ).withColumn(
        'engagement_rate',
        when(col('views') > 0, (col('likes') + col('comments')) / col('views') * 100)
        .otherwise(0)
    )
    
    # Enforce column order
    column_order = [
        'video_id', 
        'title', 
        'published_at', 
        'published_datetime',
        'publish_hour', 
        'publish_day', 
        'views', 
        'likes', 
        'comments', 
        'duration', 
        'engagement_rate'
    ]
    videos_df = videos_df.select(column_order)
    
    return videos_df

# Update the database connection details
def save_to_postgres(df, table_name):
    """Save DataFrame to PostgreSQL using environment variables"""
    # Get credentials from environment variables
    postgres_host = os.getenv('POSTGRES_HOST')
    postgres_port = os.getenv('POSTGRES_PORT')
    postgres_db = os.getenv('POSTGRES_DB')
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')
    
    # Construct JDBC URL
    jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    
    # Save to PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .option("ssl", "true") \
        .option("sslmode", "require") \
        .option("sslfactory", "org.postgresql.ssl.NonValidatingFactory") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Get latest files
    channel_file = sorted(glob.glob('data/raw/channel_data_*.json'))[-1]
    videos_file = sorted(glob.glob('data/raw/videos_data_*.json'))[-1]
    
    # Process data
    channel_df = process_channel_data(spark, channel_file)
    videos_df = process_videos_data(spark, videos_file)
    
    # Save to PostgreSQL
    save_to_postgres(channel_df, "channel_stats")
    save_to_postgres(videos_df, "video_stats")
    
    # Also save as Parquet
    channel_df.write.mode('overwrite').parquet('data/processed/channel_stats.parquet')
    videos_df.write.mode('overwrite').parquet('data/processed/video_stats.parquet')
    
    spark.stop()