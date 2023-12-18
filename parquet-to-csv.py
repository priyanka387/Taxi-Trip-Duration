import requests
import os
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime

def download_and_convert_parquet():
    base_url = "https://s3.amazonaws.com/nyc-tlc/trip+data"
    output_folder = "data"  # Change this to your desired output folder

    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Define the range of months for the year 2023
    months = range(1, 13)

    for month in months:
        # Construct the file URL
        file_name = f"yellow_tripdata_2023-01.parquet"
        file_url = f"{base_url}/{file_name}"

        # Download the Parquet file
        response = requests.get(file_url)
        if response.status_code == 200:
            # Read the Parquet file using pyarrow
            parquet_file = pq.read_table(BytesIO(response.content))

            # Convert to pandas DataFrame
            df = parquet_file.to_pandas()

            # Filter records for the year 2023
            df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
            df_2023 = df[df["tpep_pickup_datetime"].dt.year == 2023]

            # Save as CSV
            csv_file_name = f"{output_folder}/{file_name.replace('parquet', 'csv')}"
            df_2023.to_csv(csv_file_name, index=False)
            print(f"Downloaded and converted: {csv_file_name}")
        else:
            print(f"Failed to download: {file_url}")

if __name__ == "__main__":
    download_and_convert_parquet()