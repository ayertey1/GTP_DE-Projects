from pyspark.sql.functions import (
    col, when, expr, mean, sum as _sum, count
)

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