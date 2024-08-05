from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)
from airflow.models import Variable
from pprint import pprint

with DAG(
    'movie_e',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2024, 8, 1),
    catchup=True,
    tags=['project', 'movie', 'api'],
) as dag:

    run_this = EmptyOperator(
            task_id='run.this',
            )

    task_get = EmptyOperator(
            task_id='task.get',
            )

    save_data = EmptyOperator(
            task_id='save.data',
            )

    multi_y = EmptyOperator(
            task_id='multi.y',
            )

    multi_n = EmptyOperator(
            task_id='multi.n',
            )

    nation_k = EmptyOperator(
            task_id='nation.k',
            )

    nation_f = EmptyOperator(
            task_id='nation.f',
            )

    branch_op = EmptyOperator(
            task_id='branch.op',
            )

    rm_dir = EmptyOperator(
            task_id='rm.dir',
            )

    echo_task = EmptyOperator(
            task_id='echo.task',
            )
   

    throw_err = EmptyOperator(
            task_id='throw.err',
            )

    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')
    get_start = EmptyOperator(task_id="get.start",trigger_rule="all_done")
    get_end = EmptyOperator(task_id="get.end",trigger_rule="all_done" )

    get_start >> [task_get, multi_y, multi_n, nation_k, nation_f] >> get_end
    get_end >> save_data >> task_end
