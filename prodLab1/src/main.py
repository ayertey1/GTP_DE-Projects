from data_extraction import fetch_movie_data
from data_cleaning import clean_movie_data
from data_analysis import *
from visualizations import visualize_movies

# List of Movie IDs
movie_ids = [
    0, 299534, 19995, 140607, 299536, 597, 135397, 
    420818, 24428, 168259, 99861, 284054, 12445, 
    181808, 330457, 351286, 109445, 321612, 260513
]

def main():
    # Step 1: Fetch Data
    df = fetch_movie_data(movie_ids)
    
    # Step 2: Clean Data
    df_cleaned = clean_movie_data(df)
    
    # Step 3: Analyze
    df_cleaned = add_helper_columns(df_cleaned)

    print("\n Highest Revenue Movies:")
    print(top_revenue_movies(df_cleaned))
    print("\n Highest Budget Movies:")
    print(top_budget_movies(df_cleaned))
    print("\n Highest Profit Movies:")
    print(top_profit_movies(df_cleaned))
    print("\n Lowest Profit Movies:")
    print(lowest_profit_movies(df_cleaned))
    print("\n Highest ROI Movies (budget ≥ 10M):")
    print(highest_roi_movies(df_cleaned))
    print("\n Lowest ROI Movies (budget ≥ 10M):")
    print(lowest_roi_movies(df_cleaned))
    print("\n Most Voted Movies:")
    print(most_voted_movies(df_cleaned))
    print("\n Highest Rated Movies (votes ≥ 10):")
    print(highest_rated_movies(df_cleaned))
    print("\n Lowest Rated Movies (votes ≥ 10):")
    print(lowest_rated_movies(df_cleaned))
    print("\n Most Popular Movies:")
    print(most_popular_movies(df_cleaned))
    print("\n Best_scifi_action_Bruce:")
    print(search_best_scifi_action_bruce(df_cleaned))
    print("\n search_uma_thurman_tarentino:")
    print(search_uma_thurman_tarentino(df_cleaned))
    print("\n franchise_vs_standalone_performance:")
    print(franchise_vs_standalone_performance(df_cleaned))
    print("\n most_successful_franchises:")
    print(most_successful_franchises(df_cleaned))
    print("\n most_successful_directors:")
    print(most_successful_directors(df_cleaned))
    
    # Step 4: Visualize
    visualize_movies(df_cleaned)

if __name__ == "__main__":
    main()
