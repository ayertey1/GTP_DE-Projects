from pyspark.sql import SparkSession
from pyspark.sql.functions import year, split, explode,col,mean,when, to_date
import matplotlib.pyplot as plt
import seaborn as sns

# Spark session
spark = SparkSession.builder.appName("TMDBvisualize").getOrCreate()

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
    # 1. Drop rows with null genres
    df_clean = df.filter(col("genres").isNotNull())

    # 2. Split genres into arrays
    df_split = df_clean.withColumn("genres", split(col("genres"), "\|"))

    # 3. Explode genres into individual rows
    df_exploded = df_split.withColumn("genre", explode(col("genres")))

    # Now you have: title | year | roi | genres (as original array) | genre (single string)

    # 4. Collect data back to driver for plotting (Spark can't plot)
    pandas_df = df_exploded.select("genre", "roi").toPandas()

    # 5. Plotting
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
    # 1. Filter out rows with nulls in relevant columns
    df_clean = df.filter(col("vote_average").isNotNull() & col("popularity").isNotNull())

    # 2. Collect small subset (or all if manageable) to pandas
    pandas_df = df_clean.select("vote_average", "popularity").toPandas()

    # 3. Plot using seaborn
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
   # 0. Ensure release_date is of DateType (in case it's still string)
    df = df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))

    # 1. Extract year from release_date
    df = df.withColumn("release_year", year(col("release_date")))

    # 2. Group by release_year and compute means
    yearly_revenue = df.groupBy("release_year").agg(
        mean("revenue_musd").alias("revenue_musd"),
        mean("budget_musd").alias("budget_musd"),
        mean("popularity").alias("popularity")
    )

    # 3. Drop rows with nulls in key columns
    yearly_revenue_clean = yearly_revenue.dropna(subset=["revenue_musd", "budget_musd"])

    # 4. Collect to pandas for plotting
    pandas_df = yearly_revenue_clean.orderBy("release_year").toPandas()
    pandas_df.set_index("release_year", inplace=True)

    # 5. Plot
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
    # 1. Create 'is_franchise' column
    df = df.withColumn("is_franchise", col("belongs_to_collection").isNotNull())

    # 2. Group by 'is_franchise' and compute means
    franchise_vs_standalone = df.groupBy("is_franchise").agg(
        mean("revenue_musd").alias("revenue_musd"),
        mean("budget_musd").alias("budget_musd"),
        mean("popularity").alias("popularity"),
        mean("vote_average").alias("vote_average")
    )

    # 3. Convert to pandas for plotting
    pdf = franchise_vs_standalone.toPandas()

    # 4. Replace True/False with readable labels
    pdf["is_franchise"] = pdf["is_franchise"].map({True: "Franchise", False: "Standalone"})
    pdf.set_index("is_franchise", inplace=True)

    # 5. Plot bar chart
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