from datetime import datetime, timedelta
from textwrap import dedent
#from pprint import pprint as pp

from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
    BranchPythonOperator
)
import os    
from transform.trans import merge


with DAG(
    'movie_top_kr',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie_top_kr',
   # schedule_interval=timedelta(days=1),
    schedule="10 2 * * *",
    start_date=datetime(2024, 8, 1),
    catchup=True,
    tags=['movie', 'top10_in_KR'],
) as dag:

    ###REQUIREMENTS=["git+https://github.com/Nicou11/mov_agg@0.5/agg"]
    
    start = EmptyOperator(
        task_id='start'
        )


    process_movies = PythonVirtualenvOperator(
        task_id='process_movies',
        python_callable=merge,
        requirements=["git+https://github.com/test-Esther/transform@d3.0.0/trans9_12"],
        system_site_packages=False,
        op_kwargs={'load_dt': '{{ ds }}'},  # Pass execution date as load_dt
        )
    
    end = EmptyOperator(
        task_id='end'
        )
    
    # Define task dependencies
    start >> process_movies >> end
