import csv 
import os
import random
from faker import Faker
from datetime import datetime
import time

# initialize Faker
fake = Faker()

# Create output directory if it doesn't exist
OUTPUT_DIR = "data/input"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Define possible event types and product categories
EVENT_TYPES = ['view','purchase']
PRODUCT_CATEGORIES = ['electronics', 'fashion', 'home', 'sports', 'books']


def generate_event():
    """" Generate a single fake event record. """
    return {
        'user_id': random.randint(1000, 9999),
        'event_type': random.choice(EVENT_TYPES),
        'product_id': random.randint(10000, 99999),
        'product_category':random.choice(PRODUCT_CATEGORIES),
        'event_time':datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

def write_events_to_csv(file_path, num_events=10):
    """" Write multiple events to a csv file """
    with open(file_path, mode='w',newline='') as csv_file:
        fieldnames = ['user_id', 'event_type', 'product_id', 'product_category', 'event_time']
        writer = csv.DictWriter(csv_file, fieldnames= fieldnames)
        writer.writeheader

        for _ in range(num_events):
            writer.writerow(generate_event())


def main():
    file_count = 0
    while True:
        file_count += 1
        file_name = f"events_{file_count}.csv"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        print(f"Generating file: {file_path}")

        write_events_to_csv(file_path, num_events=20) # 20 events per file
        time.sleep(5)  # wait time of 5 secs to generate the next file

if __name__ == "__main__":
    main()