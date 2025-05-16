import pandas as pd
import os
from utils.logger import get_logger

logger = get_logger("validate_data")

def validate_data():
    try:
        logger.info("Starting data validation...")

        # Load CSV
        csv_path = '/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv'
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded CSV with {df.shape[0]} rows and {df.shape[1]} columns")

        # Standardize column names
        df.columns = [col.strip().replace(" ", "_").replace("&", "and").replace("(", "").replace(")", "") for col in df.columns]

        # Required Columns Check
        required_columns = [
            "Airline", "Source", "Destination",
            "Base_Fare_BDT", "Tax_and_Surcharge_BDT", "Total_Fare_BDT"
        ]
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            raise ValueError(f"Missing columns: {missing_cols}")
        logger.info("All required columns are present.")

        # Null Handling
        null_counts = df[required_columns].isnull().sum()
        if null_counts.sum() > 0:
            logger.warning(f"Null values detected:\n{null_counts}")
            df.dropna(subset=required_columns, inplace=True)
            logger.info(f"Dropped rows with nulls. Remaining rows: {len(df)}")
        else:
            logger.info("No null values in critical columns.")

        # Type Validation
        for col in ["Base_Fare_BDT", "Tax_and_Surcharge_BDT", "Total_Fare_BDT"]:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if df[col].isnull().any():
                logger.warning(f"Invalid numeric values found in {col}, dropping...")
                df = df[df[col].notnull()]
        logger.info("Fare columns validated as numeric.")

        # Negative Fare Check
        for col in ["Base_Fare_BDT", "Tax_and_Surcharge_BDT", "Total_Fare_BDT"]:
            neg_count = (df[col] < 0).sum()
            if neg_count > 0:
                logger.warning(f"{neg_count} negative values found in {col}, removing...")
                df = df[df[col] >= 0]
        logger.info("Negative fare checks completed.")

        # Basic IATA Code Validation (Source/Destination should be 3-letter uppercase codes)
        for col in ["Source", "Destination"]:
            bad_codes = df[~df[col].str.match(r'^[A-Z]{3}$')]
            if not bad_codes.empty:
                logger.warning(f"{len(bad_codes)} invalid {col} entries detected. Dropping them.")
                df = df[df[col].str.match(r'^[A-Z]{3}$')]

        logger.info("IATA code validation completed.")
        logger.info(f"Validation complete. Final row count: {len(df)}")

        # Save cleaned file for downstream processing
        df.to_csv('/opt/airflow/data/validated_data.csv', index=False)
        logger.info("Validated data saved to 'validated_data.csv'.")

    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        raise
