import pandas as pd
import os
from utils.logger import get_logger

logger = get_logger("transform_data")

def transform_data():
    try:
        logger.info("Starting data transformation...")

        input_path = '/opt/airflow/data/validated_data.csv'
        output_path = '/opt/airflow/data/transformed_data.csv'

        if not os.path.exists(input_path):
            logger.error(f"Validated input file not found: {input_path}")
            raise FileNotFoundError(input_path)

        df = pd.read_csv(input_path)
        logger.info(f"Loaded validated data with {len(df)} rows.")

        # Ensure Total_Fare_BDT is correct
        df['Computed_Total_Fare_BDT'] = df['Base_Fare_BDT'] + df['Tax_and_Surcharge_BDT']

        # Compare with existing Total_Fare_BDT
        mismatch = (df['Computed_Total_Fare_BDT'].round(2) != df['Total_Fare_BDT'].round(2)).sum()
        if mismatch > 0:
            logger.warning(f"{mismatch} rows had mismatched Total_Fare_BDT. Overwriting them.")
            df['Total_Fare_BDT'] = df['Computed_Total_Fare_BDT']

        df.drop(columns=['Computed_Total_Fare_BDT'], inplace=True)

        # Optional: round fare columns to 2 decimals
        df['Base_Fare_BDT'] = df['Base_Fare_BDT'].round(2)
        df['Tax_and_Surcharge_BDT'] = df['Tax_and_Surcharge_BDT'].round(2)
        df['Total_Fare_BDT'] = df['Total_Fare_BDT'].round(2)

        df.to_csv(output_path, index=False)
        logger.info(f"Transformation complete. Output saved to: {output_path}")

    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}")
        raise
