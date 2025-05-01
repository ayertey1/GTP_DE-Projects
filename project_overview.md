# Project Overview: Real-Time Data Ingestion Using Spark Structured Streaming & PostgreSQL

## Project Summary

This project simulates a real-time data pipeline for an e-commerce platform that tracks user interactions such as product views and purchases. The system continuously generates user event data as CSV files, processes them using Apache Spark Structured Streaming, and writes the cleaned and structured data into a PostgreSQL database.

---

## Tools & Technologies Used

| Tool/Tech                  | Purpose                                   |
|----------------------------|------------------------------------------ |
| Python                     | Event data generation (via Faker)         |
| Apache Spark               | Real-time stream processing               |
| Spark Structured Streaming | Continuous ingestion of CSVs              |
| PostgreSQL                 | Storage for processed event data          |
| Docker & Docker Compose    | Containerized environment setup           |
| PowerShell                 | Local testing and scripting               |

---

## High-Level Data Flow

```text
[Python Data Generator] 
       ↓
[data/input CSV files (on host)]
       ↓
[docker volume]
       ↓
[Spark Structured Streaming (container)]
       ↓
[Transform & Clean]
       ↓
[Write to PostgreSQL (container)]
```

* Data is generated every 5 seconds and written to `./data/input` on the host.

* Docker volume mapping exposes this folder to the Spark container at `/opt/data/input`.

* Spark reads each new file, processes it in micro-batches, and writes it to the user_events table in PostgreSQL.

* Checkpoints are maintained at `/opt/data/checkpoints/` to ensure fault tolerance.

---

## Outcomes

* Simulated a production-like streaming data pipeline end-to-end.

* Demonstrated integration of Spark Structured Streaming with a relational database (PostgreSQL).

* Containerized the entire stack using Docker for reproducibility and portability.