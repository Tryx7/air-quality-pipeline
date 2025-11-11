Here are comprehensive Grafana SQL queries for your air quality pipeline data:

## 1. Current Air Quality Dashboard Queries

### A. Current AQI for Both Cities
```sql
-- Current AQI and Health Status
SELECT 
    city,
    aqi_overall,
    health_category,
    timestamp,
    CASE 
        WHEN health_category = 'Good' THEN 1
        WHEN health_category = 'Moderate' THEN 2
        WHEN health_category = 'Unhealthy for Sensitive Groups' THEN 3
        WHEN health_category = 'Unhealthy' THEN 4
        WHEN health_category = 'Very Unhealthy' THEN 5
        WHEN health_category = 'Hazardous' THEN 6
    END as health_severity
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC, city
```

### B. Real-time Pollutant Levels
```sql
-- Current Pollutant Levels
SELECT 
    city,
    timestamp,
    pm25,
    pm10,
    ozone,
    carbon_monoxide as co,
    nitrogen_dioxide as no2,
    sulphur_dioxide as so2,
    uv_index
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC, city
```

## 2. Time Series Analysis Queries

### A. 24-Hour AQI Trend
```sql
-- 24-Hour AQI Trend by City
SELECT 
    time_bucket('1 hour', timestamp) as time,
    city,
    AVG(aqi_overall) as avg_aqi,
    MAX(aqi_overall) as max_aqi,
    MIN(aqi_overall) as min_aqi
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY time_bucket('1 hour', timestamp), city
ORDER BY time DESC, city
```

### B. 7-Day Historical AQI
```sql
-- 7-Day Historical AQI
SELECT 
    DATE(timestamp) as date,
    city,
    AVG(aqi_overall) as daily_avg_aqi,
    MAX(aqi_overall) as daily_max_aqi,
    MIN(aqi_overall) as daily_min_aqi
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '7 days'
GROUP BY DATE(timestamp), city
ORDER BY date DESC, city
```

## 3. Comparative Analysis Queries

### A. City Comparison - Current
```sql
-- City Comparison - Current AQI
SELECT 
    city,
    aqi_overall,
    health_category,
    pm25,
    pm10,
    timestamp
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY city ORDER BY timestamp DESC) as rn
    FROM air_quality_readings
    WHERE timestamp >= NOW() - INTERVAL '1 hour'
) ranked
WHERE rn = 1
ORDER BY aqi_overall DESC
```

### B. Pollutant Comparison
```sql
-- Pollutant Levels Comparison (Last 6 hours)
SELECT 
    city,
    AVG(pm25) as avg_pm25,
    AVG(pm10) as avg_pm10,
    AVG(ozone) as avg_ozone,
    AVG(carbon_monoxide) as avg_co,
    AVG(nitrogen_dioxide) as avg_no2,
    AVG(sulphur_dioxide) as avg_so2
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '6 hours'
GROUP BY city
ORDER BY city
```

## 4. Health Alert & Threshold Queries

### A. Health Alerts
```sql
-- Active Health Alerts
SELECT 
    city,
    timestamp,
    aqi_overall,
    health_category,
    pm25,
    pm10,
    CASE 
        WHEN aqi_overall > 150 THEN 'HIGH_ALERT'
        WHEN aqi_overall > 100 THEN 'MEDIUM_ALERT'
        ELSE 'NORMAL'
    END as alert_level
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '1 hour'
    AND aqi_overall > 100
ORDER BY aqi_overall DESC, city
```

### B. Threshold Violations
```sql
-- WHO Guideline Violations
SELECT 
    city,
    timestamp,
    CASE WHEN pm25 > 25 THEN 'PM2.5 Violation' END as pm25_violation,
    CASE WHEN pm10 > 50 THEN 'PM10 Violation' END as pm10_violation,
    CASE WHEN ozone > 100 THEN 'Ozone Violation' END as ozone_violation,
    CASE WHEN nitrogen_dioxide > 200 THEN 'NO2 Violation' END as no2_violation
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '24 hours'
    AND (pm25 > 25 OR pm10 > 50 OR ozone > 100 OR nitrogen_dioxide > 200)
ORDER BY timestamp DESC, city
```

## 5. Statistical Analysis Queries

### A. Daily Statistics
```sql
-- Daily Statistics
SELECT 
    city,
    DATE(timestamp) as date,
    COUNT(*) as readings_count,
    AVG(aqi_overall) as avg_aqi,
    MAX(aqi_overall) as max_aqi,
    MIN(aqi_overall) as min_aqi,
    AVG(pm25) as avg_pm25,
    AVG(pm10) as avg_pm10,
    MODE() WITHIN GROUP (ORDER BY health_category) as most_common_health_category
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '30 days'
GROUP BY city, DATE(timestamp)
ORDER BY date DESC, city
```

### B. Hourly Patterns
```sql
-- Hourly Patterns (Last 7 days)
SELECT 
    EXTRACT(HOUR FROM timestamp) as hour_of_day,
    city,
    AVG(aqi_overall) as avg_aqi,
    AVG(pm25) as avg_pm25,
    AVG(pm10) as avg_pm10
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '7 days'
GROUP BY EXTRACT(HOUR FROM timestamp), city
ORDER BY hour_of_day, city
```

## 6. Dashboard Summary Queries

### A. Dashboard Summary Stats
```sql
-- Dashboard Summary Statistics
SELECT 
    COUNT(DISTINCT city) as cities_monitored,
    COUNT(*) as total_readings,
    AVG(aqi_overall) as overall_avg_aqi,
    MAX(aqi_overall) as worst_aqi_today,
    MIN(aqi_overall) as best_aqi_today,
    COUNT(CASE WHEN aqi_overall > 100 THEN 1 END) as poor_air_quality_readings,
    ROUND(COUNT(CASE WHEN aqi_overall > 100 THEN 1 END) * 100.0 / COUNT(*), 2) as poor_air_percentage
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '24 hours'
```

### B. City Performance Ranking
```sql
-- City Performance Ranking (Last 24 hours)
SELECT 
    city,
    AVG(aqi_overall) as avg_aqi,
    MAX(aqi_overall) as max_aqi,
    COUNT(*) as readings_count,
    ROUND(AVG(CASE 
        WHEN health_category = 'Good' THEN 1
        WHEN health_category = 'Moderate' THEN 2
        WHEN health_category = 'Unhealthy for Sensitive Groups' THEN 3
        WHEN health_category = 'Unhealthy' THEN 4
        WHEN health_category = 'Very Unhealthy' THEN 5
        WHEN health_category = 'Hazardous' THEN 6
    END), 2) as avg_health_score
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY city
ORDER BY avg_aqi ASC
```

## 7. Advanced Analytics Queries

### A. Correlation Analysis
```sql
-- Pollutant Correlations
SELECT 
    CORR(pm25, pm10) as pm25_pm10_correlation,
    CORR(pm25, ozone) as pm25_ozone_correlation,
    CORR(pm10, nitrogen_dioxide) as pm10_no2_correlation
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '7 days'
```

### B. Seasonal Trends
```sql
-- Monthly Trends (Last 6 months)
SELECT 
    EXTRACT(MONTH FROM timestamp) as month,
    city,
    AVG(aqi_overall) as monthly_avg_aqi,
    AVG(pm25) as monthly_avg_pm25,
    AVG(pm10) as monthly_avg_pm10
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '6 months'
GROUP BY EXTRACT(MONTH FROM timestamp), city
ORDER BY month, city
```

## 8. Grafana Dashboard Configuration Tips

### A. Time Series Panel Settings:
- **Format as**: Time series
- **Relative time**: Last 24 hours / Last 7 days
- **Min interval**: 1h

### B. Stat Panel Settings:
- **Color mode**: Background
- **Graph mode**: None
- **Field**: Last / Mean / Max

### C. Table Panel Settings:
- **Transform**: Organize fields
- **Table display**: Enable pagination

### D. Gauge Panel Settings (for AQI):
```sql
-- Single Stat Gauge for Current AQI
SELECT 
    aqi_overall,
    health_category
FROM air_quality_readings 
WHERE city = 'nairobi'  -- Use template variable
  AND timestamp >= NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC 
LIMIT 1
```

### E. Template Variables for Dashboard:
```sql
-- City variable
SELECT DISTINCT city FROM air_quality_readings ORDER BY city

-- Time range variable
SELECT DISTINCT DATE(timestamp) as date 
FROM air_quality_readings 
ORDER BY date DESC
```

## 9. Alert Rules for Grafana

### A. High AQI Alert:
```sql
-- Alert when AQI > 150 for any city
SELECT 
    city,
    aqi_overall
FROM air_quality_readings 
WHERE timestamp >= NOW() - INTERVAL '1 hour'
  AND aqi_overall > 150
```

### B. Data Quality Alert:
```sql
-- Alert when no data for more than 2 hours
SELECT 
    COUNT(*) as missing_data_count
FROM (
    SELECT generate_series(
        NOW() - INTERVAL '3 hours', 
        NOW(), 
        '1 hour'::interval
    ) as hour
) hours
LEFT JOIN air_quality_readings aqr 
    ON DATE_TRUNC('hour', aqr.timestamp) = hours.hour
    AND aqr.city = 'nairobi'  -- Template variable
WHERE aqr.id IS NULL
```

These queries will help you build comprehensive Grafana dashboards for monitoring, analyzing, and alerting on your air quality data from the pipeline!