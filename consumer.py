import json
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

# Load .env variables
load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DB_HOST = os.getenv("DB_HOST", "postgres")


DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS heartbeats (
    id SERIAL PRIMARY KEY,
    patient_id INT,
    timestamp TIMESTAMP,
    heart_rate INT
)
""")
conn.commit()

# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    group_id='heartbeat_group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"Consuming messages from topic '{KAFKA_TOPIC}'...")

for message in consumer:
    data = message.value
    print("Received:", data)
    cursor.execute(
        "INSERT INTO heartbeats (patient_id, timestamp, heart_rate) VALUES (%s, %s, %s)",
        (data['patient_id'], data['timestamp'], data['heart_rate'])
    )
    conn.commit()
