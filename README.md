# git clone https://github.com/Chinmay325/data-engineering-project-2

An end-to-end data engineering pipeline that extracts real-time weather data from the Singapore Government API, transforms it using PySpark and Spark SQL, and loads curated analytics tables to AWS S3.

---

## Architecture

```
Singapore Weather API (5 endpoints)
        ↓
Python Extract (7 days · 20+ stations)
        ↓
AWS S3 — Raw Layer (s3://bucket/raw/weather/)
        ↓
PySpark + Spark SQL Transformations
        ↓
AWS S3 — Curated Layer (s3://bucket/curated/weather/)
```

---

## Tech Stack

- **Language:** Python 3.11
- **Big Data:** PySpark 3.5.0, Spark SQL
- **Cloud:** AWS S3
- **Libraries:** pandas, boto3, requests
- **Tools:** Jupyter Notebook, Git & GitHub

---

## Data Sources

Five real-time weather endpoints from [data.gov.sg](https://data.gov.sg):

| Endpoint | Metric | Unit |
|---|---|---|
| `/air-temperature` | Air temperature | °C |
| `/relative-humidity` | Relative humidity | % |
| `/rainfall` | Rainfall | mm |
| `/wind-speed` | Wind speed | km/h |
| `/wind-direction` | Wind direction | degrees |

---

## ETL Process

### 1. Extract
- Called 5 weather API endpoints for the last 7 days
- Collected readings from 20+ stations across Singapore
- Saved raw JSON responses to AWS S3 raw layer

### 2. Transform (PySpark + Spark SQL)
- Parsed and flattened nested JSON structure into tabular DataFrames
- Applied data cleaning: handled nulls, type casting, deduplication
- Used `createOrReplaceTempView` to run Spark SQL business logic

### 3. Load
- Saved 4 curated analytics tables to AWS S3 curated layer
- Raw and curated layers kept separate for data lake best practices

---

## Curated Tables

| Table | Description |
|---|---|
| `daily_temp.csv` | Average and max temperature per station per day |
| `hottest_stations.csv` | Top 10 hottest stations overall |
| `rainiest_days.csv` | Total rainfall per day ranked |
| `daily_humidity.csv` | Average and max humidity per day |

---

## S3 Bucket Structure

```
s3://singapore-weather-pipeline/
├── raw/
│   └── weather/
│       ├── air_temperature_raw.json
│       ├── relative_humidity_raw.json
│       ├── rainfall_raw.json
│       ├── wind_speed_raw.json
│       └── wind_direction_raw.json
└── curated/
    └── weather/
        ├── daily_temp.csv
        ├── hottest_stations.csv
        ├── rainiest_days.csv
        └── daily_humidity.csv
```

---

## How to Run

### 1. Clone the repository
```bash
git clone https://github.com/Chinmay325/data-engineering-project-2
cd data-engineering-project-2
```

### 2. Set up environment
```bash
conda create -n pyspark_env python=3.11 -y
conda activate pyspark_env
pip install pyspark==3.5.0 findspark requests boto3 pandas jupyter notebook
```

### 3. Configure AWS credentials
```bash
cp .env.example .env
# Edit .env with your actual AWS credentials
```

### 4. Run the notebook
```bash
jupyter notebook
# Open weather_pipeline_v2.ipynb and run all cells
```

---

## Environment Variables

Create a `.env` file based on `.env.example`:

```
AWS_ACCESS_KEY_ID=your-access-key-here
AWS_SECRET_ACCESS_KEY=your-secret-key-here
AWS_BUCKET=singapore-weather-pipeline
AWS_REGION=ap-south-1
```

> Never commit your actual credentials to GitHub!

---

## Key Learnings

- Handling real-time API rate limiting with retry logic
- Parsing deeply nested JSON structures in PySpark
- Separating raw and curated data lake layers in S3
- Using Spark SQL for business logic transformations
- Managing PySpark environment setup on Windows

---

## Future Improvements

- Schedule pipeline using Apache Airflow for daily runs
- Save curated layer in Parquet format for faster querying
- Add data quality checks and logging
- Build a dashboard on top of curated data using AWS QuickSight

---

## About

Built as part of a data engineering portfolio to demonstrate end-to-end pipeline development skills using industry-standard tools.
