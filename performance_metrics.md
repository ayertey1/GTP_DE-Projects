# Performance Metrics: Real-Time Data Ingestion Pipeline

This document summarizes performance observations of the Spark Structured Streaming pipeline under normal operating conditions. All tests were conducted in a local Docker-based environment on a standard development machine.

---

## Test Environment

| Component     | Spec / Version                    |
|---------------|------------------------------------|
| OS            | Windows 11 with WSL2 backend       |
| Spark         | bitnami/spark:latest (Spark 3.5.5) |
| PostgreSQL    | 13 (Docker)                        |
| Python        | 3.x + Faker                        |
| Data Generator| 20 events every 5 seconds          |
| Volume        | Host folder mounted to container   |

---

## Streaming Performance

### Micro-Batch Duration
| Batch | Rows Processed | Duration (approx) |
|-------|----------------|-------------------|
| 1     | 20             | 300–600 ms        |
| 2     | 20             | 250–550 ms        |
| 3     | 20             | 200–450 ms        |

**Observation:**  
Each micro-batch completed in under 1 second with minimal delay between file arrival and processing.

---

### Throughput

> Events per second = Total Rows / Total Time

**Example (5 files, 100 rows in 25 seconds):**

```text
100 rows / 25 seconds = 4 rows/second
```
**Average throughput:** 3–5 rows/sec under default configuration

---

### Latency (End-to-End)

| Metric                       | Estimate                    |
|------------------------------|-----------------------------|
| File generation to detection | 0–2 sec delay               |
| Detection to write in DB     | < 1 sec                     |
| Total latency                | ~1–3 sec/event              |

---

### PostgreSQL Write Health

* Insertions performed using `foreachBatch()` → `jdbc.write()`

* No connection drops or row-level errors

* Table grew as expected over time

---

### Tuning Suggestions

| Parameter                     | Current Value | Suggestion                                                    |
|-------------------------------|---------------|---------------------------------------------------------------|
| Trigger interval              | default       | Use `.trigger(processingTime="5 seconds")` for better control |
| Partitions in CSV files       | N/A (small)   | For higher volume, write partitioned files                    |
| Write mode                    | `append`      | Good for streaming                                            |
| PostgreSQL batch size         | default       | Consider `.option("batchsize", 1000)` if large scale          |

---

### Conclusion
The streaming pipeline performs well under low-throughput conditions, with sub-second latency and stable PostgreSQL inserts. The architecture is scalable for increased event volumes with minor tuning.