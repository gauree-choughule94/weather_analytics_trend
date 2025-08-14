# Weather Analytics & Trends
 
A location-based weather analytics pipeline and interactive dashboard powered by real-time and historical weather data.
Features temperature trends, rainfall predictions, wind speed analysis, and air quality metrics, visualized through geo-heatmaps, hourly forecasts, and historical climate comparisons for **data-driven weather insights**.
 
---
 
## Overview
 
**Objective:**
Enable data analysts, meteorologists, and decision-makers to monitor, analyze, and visualize weather patterns across Indian cities using an end-to-end automated pipeline.
 
---
 
## Architecture Overview
 
```plaintext
Input Data (API / External Weather Feed)
│
├── Read Source Using Python
│
├── Write Raw Data into MySQL (Filess.io)
│
├── Process Data in Apache Spark
│     ├── Missing value handling
│     ├── Derived metrics (avg_temp, temp_range)
│     ├── Weather category classification
│     └── City → State mapping (from JSON)
│
├── Write Processed Data into PostgreSQL
│
└── Visualize in Apache Superset
```
 
## Tech Stack
 
| Layer             | Technology Used                      |
| ----------------- | ------------------------------------ |
| Data Source       | Weather API / CSV feed               |
| Raw Data Storage  | MySQL (Filess.io)                    |
| Processing Engine | Apache Spark (PySpark)               |
| Data Enrichment   | City → State mapping via JSON lookup |
| Processed Storage | PostgreSQL                           |
| Visualization     | Apache Superset                      |
 
---
 
## Data Processing Workflow
 
### 1. Read & Store Raw Data
 
* Fetch data from API (or preloaded CSV)
* Store raw records into **MySQL** for persistence
 
### 2. Batch Processing in Spark
 
* Load batches by date range from MySQL
* Apply data cleaning:
 
  * Drop null-only columns
  * Fill missing values with mode
  * Round numeric columns
* Apply data enrichment:
 
  * Calculate `avg_temp`, `temp_range`, `feelslike_diff`
  * Classify into `weather_category` (Rainy, Sunny, Cloudy, Snowy, Other)
  * Add `day_of_week`, `daylight_hours`
  * Map `city → state, lat, lon` using `indian_cities.json`
 
### 3. Write Processed Data to PostgreSQL
 
* Target table: `city_weather`
 
---
 
## Visualization in Apache Superset
 
* **Time-series Line Chart:** temperature/rainfall over time
* **Area Chart:** cumulative precipitation trend
* **Heatmap:** correlation between time-of-day and temperature
 
---
 
## Folder Structure
 
```
weather-analytics/
│
├── data_lake/                              
│   ├── raw/indian_cities.json                    
│   └── API_To_MysqlDB                     
│
├── infra/                             
│   ├── docker-compose.yml             
│   ├── mysql/init.sql                         
│   └── postgres/init.sql                      
│
├── pyspark_jobs/                      
│   ├── main.py                        
│   ├── spark_utils.py              
│   ├── batch_processor.py                  
│   └── config.py                          
│
├── requirements.txt                   
└── README.md                          
```
 
---
 
## Setup & Execution
 
```bash
pip install -r requirements.txt
 
docker compose -f infra/docker-compose.yml build
docker compose -f infra/docker-compose.yml up
docker compose -f infra/docker-compose.yml down
 
spark-submit \
  --jars infra/pyspark_apps/jars/mysql-connector-j-8.3.0.jar,infra/pyspark_apps/jars/postgresql-42.7.3.jar \
  pyspark_jobs/main.py
```
 