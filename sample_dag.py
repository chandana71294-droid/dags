
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the Python function
def print_hello():
    print("Hello from Airflow DAG!")

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='sample_test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Define the task
    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )

    hello_task
