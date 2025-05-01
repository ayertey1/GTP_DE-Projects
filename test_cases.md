# Test Cases: Real-Time Data Ingestion Pipeline

This document describes manual test cases to validate the correctness and reliability of the real-time data pipeline using Spark Structured Streaming and PostgreSQL.

---

## Test Case 1: Data File Generation

**Test Name:** Generate CSV event files  
**Component:** data_generator.py  
**Steps:**
1. Run `python data_generator.py`
2. Wait 5–10 seconds
3. Check `./data/input/` directory

**Expected Result:**  
- A new file `events_<N>.csv` is created every 5 seconds
- File contains 20 rows of valid user event data with columns:
  - `user_id`, `event_type`, `product_id`, `product_category`, `event_time`

---

## Test Case 2: Spark Detects New Files

**Test Name:** Stream file detection  
**Component:** spark_streaming_to_postgres.py  
**Steps:**
1. Start Spark job inside Docker
2. Let `data_generator.py` run for at least 10 seconds
3. Observe Spark logs

**Expected Result:**  
- Spark prints log messages for each new micro-batch
- Messages like: `Writing batch 1... Writing batch 2...` appear

---

## Test Case 3: Data Transformation

**Test Name:** Schema and type enforcement  
**Component:** Spark transformation logic  
**Steps:**
1. Open a CSV file generated
2. Check data types and formats
3. Cross-check Spark logs for type parsing or schema mismatch errors

**Expected Result:**  
- No errors in Spark console
- Columns are cast to correct types in Spark (e.g., `event_time` as timestamp)

---

## Test Case 4: PostgreSQL Insertion

**Test Name:** Write to database  
**Component:** JDBC Sink  
**Steps:**
1. Start full pipeline (generator + Spark job)
2. Wait 10+ seconds
3. Connect to PostgreSQL:
   ```bash
   docker exec -it postgres_db psql -U sparkuser -d ssparkdb
   ```
4. Run:
```sql
SELECT COUNT(*) FROM user_events;
```
Expected Result:

* Row count increases with each micro-batch

* Data is correctly inserted into user_events table

## Test Case 5: Fault Tolerance
**Test Name:** Restart Spark and resume
**Component:** Spark Structured Streaming
**Steps:**

Stop Spark container while files are still being generated

Restart Spark container and rerun the job

Inspect output and checkpoint folder

**Expected Result:**

Spark resumes from last processed file

No data duplication

Checkpoints are honored from `/opt/data/checkpoints/` 

## Test Case 6: End-to-End Pipeline Health
**Test Name:** Full system run
**Component:** All
**Steps:**

Run all components as per user_guide.md

Let pipeline run for 1–2 minutes

**Expected Result:**

Files generated continuously

Spark logs show micro-batch activity

PostgreSQL row count increases with valid data

---
## Known Failure Modes

| Scenario                  | Expected Behavior                  |
|---------------------------|------------------------------------|
| CSV with missing column   | Spark should throw schema error    |
| PostgreSQL container down | Spark write should fail but retry  |
| Spark restarted           | Should resume from checkpoint      |
