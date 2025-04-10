import requests
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime

def fetch_movie_data(movie_ids, save_path="data/raw"):
    #Fetch movie data from TMDb API and save it as a CSV.
    #Timestamps intentionally added to the saved data to prevent overwriting my already extracted data
    load_dotenv()
    api_key = os.getenv('API_KEY')
    base_url = os.getenv('BASE_url')
    
    movies_data = []
    
    for movie_id in movie_ids:
        url = f'{base_url}{movie_id}?api_key={api_key}&append_to_response=credits'
        response = requests.get(url)
        if response.status_code == 200:
            movies_data.append(response.json())
        else:
            print(f"Failed to fetch movie with ID {movie_id}")
    
    movies_df = pd.DataFrame(movies_data)
    timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    save_file = f"{save_path}/moviesData_{timestamp}.csv"
    movies_df.to_csv(save_file, index=False)
    
    print(f"Data saved to {save_file}")
    return movies_df
