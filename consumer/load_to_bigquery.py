import glob
import pandas as pd
from google.cloud import bigquery

def load_parquet_to_bigquery():
    parquet_path = "/tmp/currency_rates"
    parquet_files = glob.glob(f"{parquet_path}/*.parquet")

    if not parquet_files:
        print("No parquet file found")
        return
    
    df = pd.concat([pd.read_parquet(f) for f in parquet_files])
    print(f"Loaded {len(df)} rows from {len(parquet_files)} parquet files")

    client = bigquery.Client()
    table_id = "de-weather-project-492917.currency_processed.currency_rates"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(df)} rows to {table_id}")


if __name__ == "__main__":
    load_parquet_to_bigquery()