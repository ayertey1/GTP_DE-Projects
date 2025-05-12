# Real-Time Customer Heart Beat Monitoring System

This project simulates, streams, stores, and visualizes real-time heart rate data using a full data engineering pipeline:

- **Python** for data simulation and processing
- **Apache Kafka** for real-time message streaming
- **PostgreSQL** for persistent time-series storage
- **Docker Compose** to containerize all services
- **Grafana** for interactive dashboards

---
## File Structure
```bash
.
├── Dockerfile.producer
├── Dockerfile.consumer
├── docker-compose.yml
├── data_generator.py
├── consumer.py
├── .env
├── requirements.txt
├── test_producer.py
├── test_db_insertion.py
└── README.md
```

---

## Architecture Overview

```mermaid 
graph LR
A[Data Generator (Producer)] --> B(Kafka Topic)
B --> C[Kafka Consumer]
C --> D[PostgreSQL Database]
D --> E[Grafana Dashboard]
```

---

## Services in Docker Compose

| Service    | Purpose                          | Access URL                              |
| ---------- | -------------------------------- | --------------------------------------- |
| Kafka      | Message broker                   | localhost:9092                          |
| Zookeeper  | Kafka dependency                 | -                                       |
| PostgreSQL | Heart rate data storage          | localhost:5432                          |
| pgAdmin    | Database web UI                  | [localhost:5050](http://localhost:5050) |
| Kafdrop    | Kafka topic/message viewer       | [localhost:9000](http://localhost:9000) |
| Grafana    | Data visualization dashboard     | [localhost:3000](http://localhost:3000) |
| Producer   | Simulates and streams heartbeats | Auto-runs inside Docker                 |
| Consumer   | Reads and stores data            | Auto-runs inside Docker                 |


---

## Setup Instructions

#### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/heart-monitor.git
cd heart-monitor
```
#### 2. Create a `.env` File
```bash
# Kafka
KAFKA_TOPIC=heartbeat
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# PostgreSQL
POSTGRES_DB=heart_monitor
POSTGRES_USER=****
POSTGRES_PASSWORD=****
DB_HOST=postgres
DB_PORT=5432
DB_NAME=heart_monitor
DB_USER=****
DB_PASSWORD=****

# pgAdmin
PGADMIN_DEFAULT_EMAIL=****@admin.com
PGADMIN_DEFAULT_PASSWORD=****
```
#### 3. Build and Run the Pipeline
```bash
docker-compose up --build -d
```
##### To check logs:
```bash
docker-compose logs -f producer
docker-compose logs -f consumer
```

---
## Grafana Dashboards

#### Prebuilt Panels
| Panel Name                    | Visualization | Description                            |
| ----------------------------- | ------------- | ---------------------------------------|
| Heart Rate Over Time          | Time Series   | Shows heart rate trends per patient    |
| Total Number Patients         | count         | Shows total number of patients         |
| Average Heart Rate            | Stat          | Shows average heart rate by patients   |
| Peaking Heart Rate with Time  | Stat          | Shows max heart rate with changing time|

---
![Alt text](.\Dashboard.png)
