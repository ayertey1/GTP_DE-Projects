import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
import os
import json

# Load .env variables
load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

def wait_for_kafka(max_retries=15):
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            print(f"Waiting for Kafka... attempt {i + 1}")
            time.sleep(2)
    raise Exception("Could not connect to Kafka after multiple retries")

producer = wait_for_kafka()

def generate_heartbeat():
    return {
        "patient_id": random.randint(1, 10),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "heart_rate": random.randint(60, 120)
    }

if __name__ == "__main__":
    print(f"Producing heartbeats to Kafka topic '{KAFKA_TOPIC}'...")
    while True:
        heartbeat = generate_heartbeat()
        producer.send(KAFKA_TOPIC, heartbeat)
        print("Sent:", heartbeat)
        time.sleep(2)
