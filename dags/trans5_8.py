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
    'trans_5_8',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2016, 5, 1),
    end_date=datetime(2016, 8, 31),
    catchup=True,
    tags=['movie', 'top10'],
) as dag:
    def gen_empty(*ids):
        tasks=[]
        for id in ids:
            task=EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks)

    def fun_transform(**params):
        load_dt=params['ds_nodash']
        from transform.transform5_8 import merge
        df=merge(load_dt)
        print(df)

    task_type=PythonVirtualenvOperator(
        task_id='type.cast',
        python_callable=fun_transform,
        system_site_packages=False,
        requirements="git+https://github.com/test-Esther/transform.git@d3.0.0/transform5-8",
        trigger_rule='all_done'
            )


    start, end=gen_empty('start', 'end')

    start >> task_type >> end
