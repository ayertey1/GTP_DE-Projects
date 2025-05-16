# Flight Price Analysis Pipeline – Bangladesh 🇧🇩

An automated data pipeline built with Apache Airflow to analyze flight price data for flights originating from Bangladesh. This project extracts, transforms, loads, and analyzes synthetic flight data and computes key KPIs for decision-making.

---

## Tech Stack

- **Apache Airflow** (Orchestration)
- **MySQL** (Staging DB)
- **PostgreSQL** (Analytics DB)
- **Docker & Docker Compose** (Environment)
- **Python + Pandas** (Data processing)

---

## Project Structure

```
├── dags/ # Airflow DAG definition
│ └── flight_price_pipeline.py
├── scripts/ # Python logic for ETL
│ ├── load_csv_to_mysql.py
│ ├── validate_data.py
│ ├── transform_data.py
│ ├── compute_kpis.py
│ ├── load_to_postgres.py
│ └── utils/
│ └── logger.py
├── data/
│ └── Flight_Price_Dataset_of_Bangladesh.csv
├── .env 
├── docker-compose.yaml
└── README.md
```
---

---
## DAG Overview: `flight_price_pipeline_Peter_Caleb_Ayertey`

| Task ID             | Description                                  |
|---------------------|----------------------------------------------|
| `load_csv_to_mysql` | Load CSV into MySQL staging DB               |
| `validate_data`     | Check for missing/invalid values             |
| `transform_data`    | Clean and normalize raw flight data          |
| `compute_kpis`      | Calculate KPIs: avg fare, route popularity   |
| `load_to_postgres`  | Load final KPIs into PostgreSQL analytics DB |

- **Schedule:** Every 3 hours (`0 */3 * * *`)
- **Catchup:** Disabled
- **Retries:** 2 (1-minute delay)

---

## KPI Metrics Computed

- **Average Fare by Airline**
- **Seasonal Fare Variation** (Eid, Hajj, Winter, Regular)
- **Booking Count by Airline**
- **Most Popular Routes** (Top Source-Destination pairs)

---

## Getting Started

### 1. Clone the repo

```bash
git clone https://github.com/ayertey1/GTP_DE-Projects.git
cd flight-price-pipeline
```

### 2. Create your `.env` file

### 3. Start everything with Docker
```bash
docker-compose up -d
```
### 4. create admin user for Airflow UI
```powershell
docker exec -it airflowproject-webserver-1 airflow users create --username admin --firstname Admin --lastname User --role Admin --email peter.ayertey@amalitechtraining.org --password admin123
```

## Testing the Pipeline
* Go to http://localhost:8080

* Login with:

    Username: admin

    Password: admin123

* Turn on the DAG toggle

* Wait for a scheduled run (every 3 hours) or click ▶ Trigger DAG

## Alerts
Logs are visible in the Airflow UI or directly inside container:
/opt/airflow/logs/...