import pandas as pd
import mysql.connector
import os
from utils.logger import get_logger

logger = get_logger("load_csv_to_mysql")

def load_csv_to_mysql():
    try:
        logger.info("Starting CSV load to MySQL...")

        csv_path = '/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv'
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found at {csv_path}")
            raise FileNotFoundError(csv_path)

        df = pd.read_csv(csv_path)
        logger.info(f"CSV loaded successfully with {len(df)} records and {len(df.columns)} columns.")

        # Rename columns to match SQL-safe naming
        df.columns = [col.replace(" ", "_").replace("(", "").replace(")", "").replace("&", "and") for col in df.columns]

        # Connect to MySQL
        conn = mysql.connector.connect(
            host='mysql',
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB")
        )
        cursor = conn.cursor()

        # Create table with all 18 columns
        create_table_query = """
        CREATE TABLE IF NOT EXISTS flight_prices (
            Airline VARCHAR(255),
            Source VARCHAR(10),
            Source_Name TEXT,
            Destination VARCHAR(10),
            Destination_Name TEXT,
            Departure_Date_and_Time DATETIME,
            Arrival_Date_and_Time DATETIME,
            Duration_hrs FLOAT,
            Stopovers VARCHAR(20),
            Aircraft_Type VARCHAR(100),
            Class VARCHAR(20),
            Booking_Source VARCHAR(50),
            Base_Fare_BDT FLOAT,
            Tax_and_Surcharge_BDT FLOAT,
            Total_Fare_BDT FLOAT,
            Seasonality VARCHAR(20),
            Days_Before_Departure INT
        );
        """
        cursor.execute("DROP TABLE IF EXISTS flight_prices;")  # For dev/test: reset the table !!!!!!! remove this after testing
        cursor.execute(create_table_query)
        logger.info("Staging table created/reset.")

        # Insert rows
        insert_query = """
        INSERT INTO flight_prices (
            Airline, Source, Source_Name, Destination, Destination_Name,
            Departure_Date_and_Time, Arrival_Date_and_Time, Duration_hrs,
            Stopovers, Aircraft_Type, Class, Booking_Source,
            Base_Fare_BDT, Tax_and_Surcharge_BDT, Total_Fare_BDT,
            Seasonality, Days_Before_Departure
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for index, row in df.iterrows():
            cursor.execute(insert_query, tuple(row[col] for col in df.columns))
            if index % 1000 == 0:
                logger.info(f"Inserted {index + 1} records...")

        conn.commit()
        logger.info(f"All {len(df)} records inserted into MySQL.")

        cursor.close()
        conn.close()
        logger.info("MySQL connection closed.")

    except Exception as e:
        logger.error(f"Failed to load CSV to MySQL: {str(e)}")
        raise
