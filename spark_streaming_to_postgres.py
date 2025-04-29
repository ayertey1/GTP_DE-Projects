from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col

# Create SparkSession with JDBC driver
spark = SparkSession.builder \
    .appName("SparkStructuredStreamingToPostgres") \
    .config("spark.jars", "/opt/postgresql-42.7.3.jar") \
    .getOrCreate()

# Define schema of the CSV
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("event_type", StringType()) \
    .add("product_id", IntegerType()) \
    .add("product_category", StringType()) \
    .add("event_time", TimestampType())

# Read CSVs as streaming data
input_path = "/opt/data/input"


df = spark.readStream \
    .option("header", True) \
    .schema(schema) \
    .csv(input_path)

# Optional: add transformation or filtering if needed
transformed_df = df.select(
    "user_id",
    "event_type",
    "product_id",
    "product_category",
    "event_time"
)

# PostgreSQL connection config
pg_url = "jdbc:postgresql://postgres_db:5432/ssparkdb"  # service name from docker-compose
pg_properties = {
    "user": "sparkuser",
    "password": "sparkpass",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(batch_df, batch_id):
    batch_df.write.jdbc(
        url=pg_url,
        table="user_events",
        mode="append",
        properties=pg_properties
    )

# Write each micro-batch to PostgreSQL
query = transformed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/data/checkpoints/") \
    .start()

query.awaitTermination()