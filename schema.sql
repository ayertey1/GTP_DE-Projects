CREATE TABLE IF NOT EXISTS heartbeats (
    id SERIAL PRIMARY KEY,
    patient_id INT,
    timestamp TIMESTAMP,
    heart_rate INT
)