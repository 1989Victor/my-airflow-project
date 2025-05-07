import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils import full_pipeline

default_args = {
    'owner': 'victor',
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=6)
}


new = DAG(
    dag_id="full_job",
    description="Fetching the data from the API",
    default_args=default_args
)

task1 = PythonOperator(
    dag=new,
    python_callable=full_pipeline,
    task_id="task1"
)

task1
