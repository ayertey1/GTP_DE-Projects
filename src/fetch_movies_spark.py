import os
import time
import json
from datetime import datetime
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession

def fetch_movie_data_spark(movie_ids, save_path="data/raw"):
    # Load env variables
    load_dotenv()
    api_key = os.getenv('API_KEY')
    base_url = os.getenv('BASE_url')  # should end with '/movie/'

    # Initialize Spark
    spark = SparkSession.builder.appName("FetchTMDBMovies").getOrCreate()

    movies_data = []
    for movie_id in movie_ids:
        for attempt in range(3):
            try:
                url = f'{base_url}{movie_id}?api_key={api_key}&append_to_response=credits'
                r = requests.get(url)
                if r.status_code == 200:
                    movies_data.append(r.json())
                    break
                elif r.status_code == 429:
                    print("Rate limit hit. Waiting 3 seconds...")
                    time.sleep(3)
                else:
                    print(f"Failed: {movie_id} (Status {r.status_code})")
                    time.sleep(1)
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(1)

        time.sleep(0.3)  # ~3 req/sec max

    # Convert to RDD and DataFrame
    rdd = spark.sparkContext.parallelize(movies_data)
    df = spark.read.json(rdd)

    # Save using timestamp
    timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    full_path = f"{save_path}/moviesData_{timestamp}.parquet"
    df.write.mode("overwrite").parquet(full_path)

    print(f"Data saved to {full_path}")
    return df
