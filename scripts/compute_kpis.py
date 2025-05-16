import pandas as pd
import os
from utils.logger import get_logger

logger = get_logger("compute_kpis")

def compute_kpis():
    try:
        logger.info("Starting KPI computation...")

        input_path = '/opt/airflow/data/transformed_data.csv'
        if not os.path.exists(input_path):
            logger.error(f"Transformed data not found: {input_path}")
            raise FileNotFoundError(input_path)

        df = pd.read_csv(input_path)
        logger.info(f"Loaded {len(df)} records for KPI computation.")

        # Output folder
        output_folder = '/opt/airflow/data/'
        
        # 1️. Average Fare by Airline
        kpi1 = df.groupby("Airline")["Total_Fare_BDT"].mean().reset_index()
        kpi1.rename(columns={"Total_Fare_BDT": "Average_Fare_BDT"}, inplace=True)
        kpi1.to_csv(os.path.join(output_folder, "kpi_avg_fare_by_airline.csv"), index=False)
        logger.info("KPI 1: Average fare by airline computed.")

        # 2️. Seasonal Fare Variation
        peak_seasons = ["Eid", "Winter"]
        df['Season_Type'] = df['Seasonality'].apply(lambda x: "Peak" if x in peak_seasons else "Non-Peak")
        kpi2 = df.groupby("Season_Type")["Total_Fare_BDT"].mean().reset_index()
        kpi2.rename(columns={"Total_Fare_BDT": "Average_Fare_BDT"}, inplace=True)
        kpi2.to_csv(os.path.join(output_folder, "kpi_seasonal_fare_variation.csv"), index=False)
        logger.info("KPI 2: Seasonal fare variation computed.")

        # 3️. Booking Count by Airline
        kpi3 = df['Airline'].value_counts().reset_index()
        kpi3.columns = ['Airline', 'Booking_Count']
        kpi3.to_csv(os.path.join(output_folder, "kpi_booking_count_by_airline.csv"), index=False)
        logger.info("KPI 3: Booking count by airline computed.")

        # 4. Most Popular Routes
        df['Route'] = df['Source'] + " → " + df['Destination']
        kpi4 = df['Route'].value_counts().reset_index()
        kpi4.columns = ['Route', 'Booking_Count']
        kpi4.to_csv(os.path.join(output_folder, "kpi_popular_routes.csv"), index=False)
        logger.info("KPI 4: Most popular routes computed.")

        logger.info("All KPIs computed and saved successfully.")

    except Exception as e:
        logger.error(f"KPI computation failed: {str(e)}")
        raise
