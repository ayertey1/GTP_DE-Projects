import pandas as pd
import psycopg2
import os
from utils.logger import get_logger

logger = get_logger("load_to_postgres")

def load_to_postgres():
    try:
        logger.info("Starting load of KPIs to PostgreSQL...")

        # PostgreSQL connection
        conn = psycopg2.connect(
            host="postgres",
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            dbname=os.getenv("POSTGRES_DB")
        )
        cursor = conn.cursor()

        data_path = '/opt/airflow/data/'

        # Define KPI files and target tables
        kpis = [
            {
                "file": "kpi_avg_fare_by_airline.csv",
                "table": "kpi_avg_fare_by_airline",
                "schema": """
                    CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
                        Airline VARCHAR(255),
                        Average_Fare_BDT FLOAT
                    );
                """
            },
            {
                "file": "kpi_seasonal_fare_variation.csv",
                "table": "kpi_seasonal_fare_variation",
                "schema": """
                    CREATE TABLE IF NOT EXISTS kpi_seasonal_fare_variation (
                        Season_Type VARCHAR(20),
                        Average_Fare_BDT FLOAT
                    );
                """
            },
            {
                "file": "kpi_booking_count_by_airline.csv",
                "table": "kpi_booking_count_by_airline",
                "schema": """
                    CREATE TABLE IF NOT EXISTS kpi_booking_count_by_airline (
                        Airline VARCHAR(255),
                        Booking_Count INT
                    );
                """
            },
            {
                "file": "kpi_popular_routes.csv",
                "table": "kpi_popular_routes",
                "schema": """
                    CREATE TABLE IF NOT EXISTS kpi_popular_routes (
                        Route VARCHAR(255),
                        Booking_Count INT
                    );
                """
            }
        ]

        for kpi in kpis:
            file_path = os.path.join(data_path, kpi["file"])
            if not os.path.exists(file_path):
                logger.warning(f"KPI file not found: {file_path}. Skipping...")
                continue

            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} rows from {file_path}.")

            # Create table
            cursor.execute(f"DROP TABLE IF EXISTS {kpi['table']};")
            cursor.execute(kpi["schema"])
            logger.info(f"Created table {kpi['table']}.")

            # Insert rows
            for _, row in df.iterrows():
                placeholders = ', '.join(['%s'] * len(row))
                cursor.execute(
                    f"INSERT INTO {kpi['table']} VALUES ({placeholders})",
                    tuple(row)
                )

            conn.commit()
            logger.info(f"Inserted data into {kpi['table']} successfully.")

        cursor.close()
        conn.close()
        logger.info("PostgreSQL KPI load complete.")

    except Exception as e:
        logger.error(f"Failed to load KPIs into PostgreSQL: {str(e)}")
        raise
