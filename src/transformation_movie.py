from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, udf, size, array_sort, split, when, lit, 
    regexp_replace, expr, concat_ws, to_date, monotonically_increasing_id
)
from pyspark.sql.types import *

# Load environment
load_dotenv()

# Spark session
spark = SparkSession.builder.appName("TMDBCleaning").getOrCreate()

# Load extracted data
df = spark.read.parquet("data/raw/moviesData_*.parquet")

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
