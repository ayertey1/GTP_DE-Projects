#                           ---------------- Importing Dependencies Section ----------------
import os
import time
import json
from datetime import datetime
import requests
from pyspark.sql.functions import (
    col, size, when, expr, concat_ws, to_date, mean, sum as _sum, count,year, split, explode
)
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.types import *
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("AllTMDBMovies").getOrCreate()



#                                ---------------- Data Extraction Section ----------------


def fetch_movie_data_spark(movie_ids, save_path="data/raw"):
    # Load env variables
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

        time.sleep(0.3)  # ~3 req/sec max

    # Convert to RDD and DataFrame
    rdd = spark.sparkContext.parallelize([json.dumps(movie) for movie in movies_data])
    df = spark.read.json(rdd)

    # Save using timestamp
    timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    full_path = f"{save_path}/moviesData_{timestamp}.parquet"
    df.coalesce(1).write.mode("overwrite").parquet(full_path)

    print(f"Data saved to {full_path}")
    return df


#                                       ---------------- Data Cleaning Section ----------------
def dataRefine(df):
    # Drop unnecessary columns
    drop_cols = ["adult", "imdb_id", "original_title", "video", "homepage"]
    df = df.drop(*drop_cols)

    # Parse nested JSON fields
    #genre
    df = df.withColumn("genres", concat_ws("|", expr("transform(genres, x -> x.name)")))

    #belongs_to_collection
    df = df.withColumn("belongs_to_collection", col("belongs_to_collection.name"))

    #production_countries
    df = df.withColumn("production_countries", expr("array_join(transform(production_countries, x -> x.name), '|')"))

    #production_companies
    df = df.withColumn("production_companies", expr("array_join(transform(production_companies, x -> x.name), '|')"))

    #spoken_languages
    df = df.withColumn("spoken_languages", expr("array_join(transform(spoken_languages, x -> x.english_name), '|')"))

    # Extract cast, director, cast size, and crew size from credits
    # Extract relevant info
    df = df.withColumn("cast", expr("array_join(transform(credits.cast, x -> x.name), '|')"))
    df = df.withColumn("cast_size", size("credits.cast"))
    df = df.withColumn("crew_size", size("credits.crew"))

    # Safely extract director — check for empty array
    df = df.withColumn("director", expr("""
        CASE 
            WHEN size(filter(credits.crew, x -> x.job = 'Director')) > 0 
            THEN filter(credits.crew, x -> x.job = 'Director')[0].name 
            ELSE NULL 
        END
    """))

    # Drop the original credits field
    df = df.drop("credits")

    # Clean up multiple-value columns (sorted order)
    for col_name in ["genres", "production_countries", "spoken_languages"]:
        df = df.withColumn(
            col_name,
            expr(f"array_join(array_sort(transform(split({col_name}, '\\\\|'), x -> trim(x))), '|')")
        )

    # Convert numeric fields
    num_cols = ["budget", "popularity", "id", "revenue", "runtime"]
    for field in num_cols:
        df = df.withColumn(field, col(field).cast("double"))

    df = df.withColumn("release_date", to_date("release_date", "yyyy-MM-dd"))

    # Replace zero with null
    for col_name in ["budget", "revenue", "runtime"]:
        df = df.withColumn(col_name, when(col(col_name) == 0, None).otherwise(col(col_name)))

    # Add new columns
    df = df.withColumn("budget_musd", col("budget") / 1e6)
    df = df.withColumn("revenue_musd", col("revenue") / 1e6)

    # Vote count zero cleanup
    df = df.withColumn("vote_average", when(col("vote_count") == 0, None).otherwise(col("vote_average")))

    # Missing data handling
    df = df.withColumn("overview", when(col("overview").isin("No Data", "", "nan"), None).otherwise(col("overview")))
    df = df.withColumn("tagline", when(col("tagline").isin("No Data", "", "nan"), None).otherwise(col("tagline")))

    # Drop duplicates and filter final rows
    df = df.dropDuplicates()
    df = df.dropna(subset=["id", "title"])
    important_cols = [
        "id", "title", "tagline", "release_date", "genres", "belongs_to_collection",
        "original_language", "budget_musd", "revenue_musd", "production_companies",
        "production_countries", "vote_count", "vote_average", "popularity", "runtime",
        "overview", "spoken_languages", "cast", "cast_size", "director", "crew_size"
    ]
    df = df.withColumn(
        "non_nulls",
        expr(f"size(filter(array({','.join(important_cols)}), x -> x is not null))")
    )
    df = df.filter(col("non_nulls") >= 10).drop("non_nulls")
    df = df.filter(col("status") == "Released").drop("status")

    # Final columns
    df = df.select([
        "id", "title", "tagline", "release_date", "genres", "belongs_to_collection",
        "original_language", "budget_musd", "revenue_musd", "production_companies",
        "production_countries", "vote_count", "vote_average", "popularity", "runtime",
        "overview", "spoken_languages", "poster_path", "cast", "cast_size", "director", "crew_size"
    ])

    # Output final count
    print(f"Cleaned DataFrame has {df.count()} rows and {len(df.columns)} columns.")

    return df



#                                   ---------------- Data Analysis Section ----------------


# Add Helper KPI Columns (Profit & ROI) --
def add_helper_columns(df):
    df = df.withColumn("profit", col("revenue_musd") - col("budget_musd"))
    df = df.withColumn("roi", col("revenue_musd") / col("budget_musd"))
    df = df.withColumn("roi",when(col("budget_musd") > 0, col("revenue_musd") / col("budget_musd")).otherwise(None))
    return df

# --- Helper Function: Rank Movies ---
def rank_movies_spark(df, sort_by, ascending=False, min_budget_musd=None, min_votes=None, top_n=5):
    ranked_df = df

    if min_budget_musd is not None:
        ranked_df = ranked_df.filter(col("budget_musd") >= min_budget_musd)

    if min_votes is not None:
        ranked_df = ranked_df.filter(col("vote_count") >= min_votes)

    order_col = col(sort_by).asc() if ascending else col(sort_by).desc()

    ranked_df = ranked_df.orderBy(order_col).select("title", sort_by).limit(top_n)

    return ranked_df

# --- Advanced Search Queries ---
def search_best_scifi_action_bruce(df):
    """
    Find the best-rated Science Fiction Action movies starring Bruce Willis.
    """
    query1 = df.filter(
        col("genres").contains("Science Fiction") &
        col("genres").contains("Action") &
        col("cast").contains("Bruce Willis")
    ).orderBy(col("vote_average").desc())
    
    return query1.select("title", "vote_average").show(truncate=False)


def search_uma_thurman_tarentino(df):
    """
    Find movies starring Uma Thurman directed by Quentin Tarantino.
    """
    query2 = df.filter(
        col("cast").contains("Uma Thurman") &
        col("director").contains("Quentin Tarantino")
    ).orderBy(col("runtime").asc())
    
    return query2.select("title", "runtime").show(truncate=False)


# --- Franchise vs Standalone Analysis ---
def franchise_vs_standalone_performance(df):
    """
    Compare franchise vs standalone movies by revenue, ROI, budget, popularity, and rating.
    """
    df = df.withColumn("is_franchise", col("belongs_to_collection").isNotNull())

    # Group by is_franchise and calculate aggregates
    franchise_stats = df.groupBy("is_franchise").agg(
        mean("revenue_musd").alias("avg_revenue_musd"),
        expr("percentile_approx(roi, 0.5)").alias("median_roi"),
        mean("budget_musd").alias("avg_budget_musd"),
        mean("popularity").alias("avg_popularity"),
        mean("vote_average").alias("avg_vote_average")
    )

    # Rename True/False as Franchise/Standalone 
    franchise_stats = franchise_stats.withColumn(
        "is_franchise",
        when(col("is_franchise") == True, "Franchise").otherwise("Standalone")
    )
    return franchise_stats.orderBy("is_franchise").show(truncate=False)

# --- Most Successful Franchises ---
def most_successful_franchises(df, top_n=5):
    # Create is_franchise column based on whether 'belongs_to_collection' is not null
    df = df.withColumn("is_franchise", col("belongs_to_collection").isNotNull())

    # Group by is_franchise and calculate aggregates
    franchise_stats = df.groupBy("is_franchise").agg(
        mean("revenue_musd").alias("avg_revenue_musd"),
        expr("percentile_approx(roi, 0.5)").alias("median_roi"),
        mean("budget_musd").alias("avg_budget_musd"),
        mean("popularity").alias("avg_popularity"),
        mean("vote_average").alias("avg_vote_average")
    )

    # Rename True/False as Franchise/Standalone 
    franchise_stats = franchise_stats.withColumn(
        "is_franchise",
        when(col("is_franchise") == True, "Franchise").otherwise("Standalone")
    )

    return franchise_stats.orderBy("is_franchise").show(truncate=False)

# --- Most Successful Directors ---
def most_successful_directors(df, top_n=5):
    # Filter to only rows where 'belongs_to_collection' is not null
    franchise_success = df.filter(col("belongs_to_collection").isNotNull())

    # Group by the franchise name
    franchise_success = franchise_success.groupBy("belongs_to_collection").agg(
        count("id").alias("num_movies"),
        _sum("budget_musd").alias("total_budget_musd"),
        mean("budget_musd").alias("avg_budget_musd"),
        _sum("revenue_musd").alias("total_revenue_musd"),
        mean("revenue_musd").alias("avg_revenue_musd"),
        mean("vote_average").alias("avg_vote_average")
    )

    # Sort by total revenue
    franchise_success = franchise_success.orderBy(col("total_revenue_musd").desc())
 
    return franchise_success.show(5, truncate=False)



#                                   ---------------- Data Visualization Section ----------------



def plot_revenue_vs_budget(df):
    """Plot Revenue vs Budget trends."""
    # Add release_year column
    df = df.withColumn("release_year", year("release_date"))
    # Filter out null values to avoid plotting issues
    plot_df = df.select("budget_musd", "revenue_musd") \
                .dropna(subset=["budget_musd", "revenue_musd"]) \
                .toPandas()
    # Set plot style
    sns.set(style="whitegrid")
    # Plot
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=plot_df, x="budget_musd", y="revenue_musd")
    plt.title("Revenue vs Budget")
    plt.xlabel("Budget (Million USD)")
    plt.ylabel("Revenue (Million USD)")
    plt.xscale("log")
    plt.yscale("log")
    plt.grid(True, which="both", ls="--", linewidth=0.5)
    plt.tight_layout()
    plt.show()

def plot_roi_distribution_by_genre(df):
    """Plot ROI Distribution by Genre."""
    # Drop rows with null genres
    df_clean = df.filter(col("genres").isNotNull())
    # Split genres into arrays
    df_split = df_clean.withColumn("genres", split(col("genres"), "\|"))

    # Explode genres into individual rows
    df_exploded = df_split.withColumn("genre", explode(col("genres")))

    # Now you have: title | year | roi | genres (as original array) | genre (single string)

    # Collect data back to driver for plotting (Spark can't plot)
    pandas_df = df_exploded.select("genre", "roi").toPandas()

    # Plotting
    plt.figure(figsize=(14, 8))
    sns.boxplot(data=pandas_df, x='genre', y='roi')
    plt.title('ROI Distribution by Genre')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Return on Investment (ROI)')
    plt.grid(True, ls="--", linewidth=0.5)
    plt.tight_layout()
    plt.show()

def plot_popularity_vs_rating(df):
    """Plot Popularity vs Rating."""
    # Filter out rows with nulls in relevant columns
    df_clean = df.filter(col("vote_average").isNotNull() & col("popularity").isNotNull())

    # Collect small subset (or all if manageable) to pandas
    pandas_df = df_clean.select("vote_average", "popularity").toPandas()

    # Plot using seaborn
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=pandas_df, x='vote_average', y='popularity')
    plt.title('Popularity vs Rating')
    plt.xlabel('Average Rating')
    plt.ylabel('Popularity')
    plt.grid(True, ls="--", linewidth=0.5)
    plt.tight_layout()
    plt.show()

def plot_yearly_box_office_trends(df):
    """Plot Yearly Trends in Box Office Performance."""
   # Ensure release_date is of DateType (in case it's still string)
    df = df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))

    # Extract year from release_date
    df = df.withColumn("release_year", year(col("release_date")))

    # Group by release_year and compute means
    yearly_revenue = df.groupBy("release_year").agg(
        mean("revenue_musd").alias("revenue_musd"),
        mean("budget_musd").alias("budget_musd"),
        mean("popularity").alias("popularity")
    )

    # Drop rows with nulls in key columns
    yearly_revenue_clean = yearly_revenue.dropna(subset=["revenue_musd", "budget_musd"])

    # Collect to pandas for plotting
    pandas_df = yearly_revenue_clean.orderBy("release_year").toPandas()
    pandas_df.set_index("release_year", inplace=True)

    # Plot
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=pandas_df, x=pandas_df.index, y="revenue_musd", label="Revenue")
    sns.lineplot(data=pandas_df, x=pandas_df.index, y="budget_musd", label="Budget")
    plt.title("Yearly Trends: Revenue and Budget")
    plt.xlabel("Year")
    plt.ylabel("Million USD")
    plt.legend()
    plt.grid(True, ls="--", linewidth=0.5)
    plt.tight_layout()
    plt.show()

def plot_franchise_vs_standalone(df):
    """Plot Franchise vs Standalone Success."""
    # Create 'is_franchise' column
    df = df.withColumn("is_franchise", col("belongs_to_collection").isNotNull())

    # Group by 'is_franchise' and compute means
    franchise_vs_standalone = df.groupBy("is_franchise").agg(
        mean("revenue_musd").alias("revenue_musd"),
        mean("budget_musd").alias("budget_musd"),
        mean("popularity").alias("popularity"),
        mean("vote_average").alias("vote_average")
    )

    # Convert to pandas for plotting
    pdf = franchise_vs_standalone.toPandas()

    # Replace True/False with readable labels
    pdf["is_franchise"] = pdf["is_franchise"].map({True: "Franchise", False: "Standalone"})
    pdf.set_index("is_franchise", inplace=True)

    # Plot bar chart
    pdf[["revenue_musd", "budget_musd"]].plot(kind="bar", figsize=(10, 6))
    plt.title("Franchise vs Standalone: Revenue and Budget Comparison")
    plt.ylabel("Million USD")
    plt.xticks(rotation=0)
    plt.grid(True, ls="--", linewidth=0.5)
    plt.tight_layout()
    plt.show()

def visualize_movies(df):
    """Run all visualizations in sequence."""
    sns.set(style="whitegrid")
    
    plot_revenue_vs_budget(df)
    plot_roi_distribution_by_genre(df)
    plot_popularity_vs_rating(df)
    plot_yearly_box_office_trends(df)
    plot_franchise_vs_standalone(df)