# Quick Reference Guide - Stock Market ETL Pipeline

## 🚀 Setup Commands

```bash
# Initial setup
chmod +x setup.sh
./setup.sh

# Manual setup
mkdir -p dags logs config plugins data/{raw,processed}
docker-compose build
docker-compose up airflow-init
docker-compose up -d
```

## 🔧 Daily Operations

### Start/Stop Services
```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# Restart specific service
docker-compose restart airflow-scheduler

# View service status
docker-compose ps
```

### View Logs
```bash
# All logs (follow mode)
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-apiserver

# Last 100 lines
docker-compose logs --tail=100 airflow-scheduler
```

## 📊 Access Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Grafana | http://localhost:3000 | admin / admin |

## 🔍 Debugging Commands

### Test DAG
```bash
# Check DAG syntax
docker-compose exec airflow-scheduler python /opt/airflow/dags/stock_market_dag.py

# List all DAGs
docker-compose exec airflow-scheduler airflow dags list

# Test specific task
docker-compose exec airflow-scheduler airflow tasks test stock_market_etl_pipeline extract_stock_data 2024-10-14
```

### Check Database
```bash
# Test PostgreSQL connection
docker-compose exec airflow-scheduler python -c "
import psycopg2, os
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    database=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    sslmode='require'
)
print('✓ Connected')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM stock_market_data')
print(f'Records: {cursor.fetchone()[0]}')
"
```

### Access Container Shell
```bash
# Access scheduler container
docker-compose exec airflow-scheduler bash

# Access database container
docker-compose exec postgres psql -U airflow
```

## 🔄 Common Fixes

### Reset DAG
```bash
# Clear all task instances
docker-compose exec airflow-scheduler airflow dags backfill -s 2024-01-01 -e 2024-01-01 --reset-dagruns stock_market_etl_pipeline

# Clear specific task
docker-compose exec airflow-scheduler airflow tasks clear stock_market_etl_pipeline -t extract_stock_data -y
```

### Fix Permission Issues
```bash
sudo chown -R $USER:$USER dags logs config plugins data
echo "AIRFLOW_UID=$(id -u)" >> .env
docker-compose restart
```

### Rebuild Everything
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## 📝 Configuration Files

| File | Purpose | Location |
|------|---------|----------|
| `.env` | Environment variables | Project root |
| `stock_market_dag.py` | Main DAG definition | `dags/` |
| `docker-compose.yaml` | Service definitions | Project root |
| `requirements.txt` | Python dependencies | Project root |

## 🎯 Key Environment Variables

```bash
# Required
POLYGON_API_KEY=your_api_key_here
POSTGRES_HOST=your_host
POSTGRES_PORT=your_port
POSTGRES_DB=your_database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password

# Optional
POSTGRES_SSL_MODE=require
AIRFLOW_UID=1000
```

## 📈 Monitoring Queries

```sql
-- Check latest data
SELECT symbol, MAX(trade_date) as latest_date, COUNT(*) as records
FROM stock_market_data
GROUP BY symbol;

-- Today's performance
SELECT symbol, close_price, price_change_pct
FROM stock_market_data
WHERE trade_date::date = CURRENT_DATE
ORDER BY price_change_pct DESC;

-- Data quality check
SELECT data_quality, COUNT(*) as count
FROM stock_market_data
GROUP BY data_quality;
```

## 🚨 Emergency Commands

```bash
# Stop everything immediately
docker-compose down

# Nuclear option (deletes all data)
docker-compose down -v
docker system prune -af
rm -rf logs/* data/raw/* data/processed/*

# Fresh start
docker-compose up airflow-init
docker-compose up -d
```

## 📞 Health Checks

```bash
# Check Airflow scheduler health
curl http://localhost:8974/health

# Check Airflow API server health  
curl http://localhost:8080/health

# Check PostgreSQL
docker-compose exec postgres pg_isready -U airflow

# Check Redis
docker-compose exec redis redis-cli ping
```

## 🔐 Security Checklist

- [ ] `.env` file is in `.gitignore`
- [ ] Changed default Airflow password
- [ ] Using SSL for PostgreSQL
- [ ] API keys are rotated regularly
- [ ] Logs don't contain sensitive data

## 📦 File Locations

```
Project Root/
├── dags/              # DAG definitions
├── logs/              # Airflow logs
├── config/            # Airflow config
├── plugins/           # Custom plugins
├── data/
│   ├── raw/          # Raw JSON backups
│   └── processed/    # Parquet files
└── .env              # Environment variables (DON'T COMMIT!)
```

## 🎓 Learning Resources

- [Airflow Docs](https://airflow.apache.org/docs/)
- [Polygon API Docs](https://polygon.io/docs)
- [PostgreSQL Docs](https://www.postgresql.org/docs/)
- [Docker Compose Docs](https://docs.docker.com/compose/)

## 💡 Pro Tips

1. **Use `docker-compose logs -f` to monitor in real-time**
2. **Test individual tasks before running full DAG**
3. **Check data/ directory for backup files**
4. **Set up Grafana dashboards for visualization**
5. **Monitor disk space - logs can grow large**
6. **Use Airflow UI task logs for detailed debugging**

## ⚡ Performance Tuning

```python
# In stock_market_dag.py, adjust:

# Reduce stocks for faster execution
STOCK_SYMBOLS = ['AAPL', 'GOOGL']  # Instead of 5+

# Change schedule
schedule='@daily'  # Instead of specific time

# Adjust retries
'retries': 1,  # Instead of 3
```

## 🔔 Set Up Alerts

```python
# Add to default_args in DAG
'email': ['your@email.com'],
'email_on_failure': True,
'email_on_retry': False,
```

---

**Remember:** Always check logs first when troubleshooting!

```bash
docker-compose logs -f airflow-scheduler
```