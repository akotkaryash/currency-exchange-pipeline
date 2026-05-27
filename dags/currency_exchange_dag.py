from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
'owner': 'yash', # Who owns this DAG
'retries': 2, # If a task fails, retry 2 times
'retry_delay': timedelta(minutes=5), # Wait 5 minutes between retries
}

dag = DAG(
    'currency_exchange_pipeline',
    default_args=default_args,
    description='Load currency rate to bigquery daily',
    schedule='0 2 * * *',
    start_date=datetime(2026, 5, 27),
    catchup=False
)

def load_to_bigquery():
    import subprocess
    result = subprocess.run(['docker', 'exec', 'currency-exchange-pipeline-consumer-1', 'python', '/app/load_to_bigquery.py'], capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"Loader failed: {result.stderr}")
    print(result.stdout)

load_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag
)

load_task