from fetch_movies_spark import fetch_movie_data_spark

movie_ids = [
    0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428,
    168259, 99861, 284054, 12445, 181808, 330457, 351286, 109445,
    321612, 260513
]

df = fetch_movie_data_spark(movie_ids)
df.show(truncate=False)
