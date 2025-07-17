from pyspark.sql import SparkSession
from dataExtraction import fetch_movie_data_spark
from dataRefinery import dataRefine
from dataAnalysis import *
from visualizations import visualize_movies
from dotenv import load_dotenv

# Spark session
spark = SparkSession.builder.appName("TMDBmain").getOrCreate()

#Load environment
load_dotenv()

#List of movie IDs
movie_ids = [
    0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428,
    168259, 99861, 284054, 12445, 181808, 330457, 351286, 109445,
    321612, 260513
]
def main():
    # ---- extracts data from movies API ----
    df = fetch_movie_data_spark(movie_ids)


    # ---- Refines or cleans data ----
    # Load extracted data
    df = spark.read.parquet("data/raw/moviesData_*.parquet")
    df = dataRefine(df)


    # ---- Exploratory Data Analysis ----

    df = add_helper_columns(df)

    # KPI Calculations
    # Highest Revenue
    print("\nTop 5 Highest Revenue Movies:")
    rank_movies_spark(df, sort_by='revenue_musd', ascending=False).show(truncate=False)

    # Highest Budget
    print("\nTop 5 Highest Budget Movies:")
    rank_movies_spark(df, sort_by='budget_musd', ascending=False).show(truncate=False)

    # Highest Profit
    print("\nTop 5 Highest Profit Movies:")
    rank_movies_spark(df, sort_by='profit', ascending=False).show(truncate=False)

    # Lowest Profit
    print("\nTop 5 Lowest Profit Movies:")
    rank_movies_spark(df, sort_by='profit', ascending=True).show(truncate=False)

    # Highest ROI (only budget ≥ 10M)
    print("\nTop 5 Highest ROI Movies (budget ≥ 10M):")
    rank_movies_spark(df, sort_by='roi', ascending=False, min_budget_musd=10).show(truncate=False)

    # Lowest ROI (only budget ≥ 10M)
    print("\nTop 5 Lowest ROI Movies (budget ≥ 10M):")
    rank_movies_spark(df, sort_by='roi', ascending=True, min_budget_musd=10).show(truncate=False)

    # Most Voted Movies
    print("\nTop 5 Most Voted Movies:")
    rank_movies_spark(df, sort_by='vote_count', ascending=False).show(truncate=False)

    # Highest Rated Movies (only movies ≥ 10 votes)
    print("\nTop 5 Highest Rated Movies (votes ≥ 10):")
    rank_movies_spark(df, sort_by='vote_average', ascending=False, min_votes=10).show(truncate=False)

    # Lowest Rated Movies (only movies ≥ 10 votes)
    print("\nTop 5 Lowest Rated Movies (votes ≥ 10):")
    rank_movies_spark(df, sort_by='vote_average', ascending=True, min_votes=10).show(truncate=False)

    # Most Popular Movies
    print("\nTop 5 Most Popular Movies:")
    rank_movies_spark(df, sort_by='popularity', ascending=False).show(truncate=False)

    print("\n Best_scifi_action_Bruce:")
    print(search_best_scifi_action_bruce(df))
    print("\n search_uma_thurman_tarentino:")
    print(search_uma_thurman_tarentino(df))
    print("\n franchise_vs_standalone_performance:")
    print(franchise_vs_standalone_performance(df))
    print("\n most_successful_franchises:")
    print(most_successful_franchises(df))
    print("\n most_successful_directors:")
    print(most_successful_directors(df))


    #---- Visualizations ----
    visualize_movies(df)




if __name__ == "__main__":
    main()



