from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
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

with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie_extract9_12',
   # schedule_interval=timedelta(days=1),
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 31),
    catchup=True,
    tags=['movie', '2016', 'extract9_12'],
) as dag:

    def pic():
        from extract.icebreaking import pic
        pic()

        
    branch_op = PythonVirtualenvOperator(
        task_id="branch.op",
	    python_callable=pic,
        requirements=["git+https://github.com/test-Esther/extract.git@release/d1.0.0"],
        ) 

    get_data = PythonVirtualenvOperator(
        task_id="get.data",
        python_callable=pic,
        requirements=["git+https://github.com/test-Esther/extract.git@release/d1.0.0"],
        system_site_packages=False,
        )

    save_data = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=pic,
        requirements=["git+https://github.com/test-Esther/extract.git@release/d1.0.0"],
        system_site_packages=False,
        )

    start = PythonVirtualenvOperator(
        task_id="start",
        python_callable=pic,
        requirements=["git+https://github.com/test-Esther/extract.git@release/d1.0.0"],
        system_site_packages=False,
        )

    end = PythonVirtualenvOperator(
        task_id="end",
        python_callable=pic,
        requirements=["git+https://github.com/test-Esther/extract.git@release/d1.0.0"],
        system_site_packages=False,
        )
    

    rm_dir = BashOperator(
	    task_id='rm.dir',
	    bash_command='rm -rf ~/tmp/jkl_parquet/load_dt={{ ds_nodash }}'
        )

    get_start = EmptyOperator(task_id='get.start', trigger_rule="all_done")
    get_end = EmptyOperator(task_id='get.end')


    start >> branch_op

    branch_op >> rm_dir >> get_start
    branch_op >> get_start >> get_data >> get_end
    
    get_end >> save_data >> end
    
