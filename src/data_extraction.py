import requests
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
import time

def fetch_movie_data(movie_ids, save_path="data/raw"):
    #Fetch movie data from TMDb API and save it as a CSV.
    #Timestamps intentionally added to the saved data to prevent overwriting my already extracted data
    #load api keys and base urls from .env file
    load_dotenv()
    api_key = os.getenv('API_KEY')
    base_url = os.getenv('BASE_url')


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


        time.sleep(0.3)  # ~3 requests per second max to be safe
    #create Data frame
    movies_df = pd.DataFrame(movies_data)
    
    # saving data using time stamps to prevent over_writing data everytime notebook runs.
    timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    movies_df.to_csv(f'./data/raw/moviesData_{timestamp}.csv', index=False)

    
    # print(f"Data saved to {save_file}")
    return movies_df
