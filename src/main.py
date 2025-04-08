import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os

# Your TMDb API key
load_dotenv()
api_key = os.getenv('API_KEY')

# Base URL for TMDb API
base_url = os.getenv('BASE_url')

# List of movie IDs
movie_ids = [
    0, 299534, 19995, 140607, 299536, 597, 135397, 
    420818, 24428, 168259, 99861, 284054, 12445, 
    181808, 330457, 351286, 109445, 321612, 260513
]

# Empty list to store movie data
movies_data = []

# Loop through each movie ID and fetch data
for movie_id in movie_ids:
    url = f'{base_url}{movie_id}?api_key={api_key}&append_to_response=credits'
    response = requests.get(url)
    
    if response.status_code == 200:
        movie_info = response.json()
        movies_data.append(movie_info)
    else:
        print(f"Failed to fetch movie with ID {movie_id}")

# Convert list of movies to a pandas DataFrame
movies_df = pd.DataFrame(movies_data)

# Display the DataFrame
print(movies_df.head())

# Save to CSV 
timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
movies_df.to_csv(f'data/raw/moviesData_{timestamp}.csv', index=False)
