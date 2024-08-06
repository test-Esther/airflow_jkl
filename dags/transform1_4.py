from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (PythonOperator, PythonVirtualenvOperator, BranchPythonOperator)

from airflow.models import Variable
from pprint import pprint as pp

with DAG(
    'Transform1-4',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
#    max_active_runs= 1,
#    max_active_tasks=3,
    description='About movie',
    schedule="10 2 * * *",
    start_date=datetime(2016, 1, 1),
    end_date=datetime(2016, 5, 1),
    catchup=True,
    tags=['movie', '2016', 'transform1-4'],
) as dag:
    def gen_empty(*ids):
        tasks=[]
        for id in ids:
            task=EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks)

    def fun_transform(task_name, **params):
        load_dt=params['ds_nodash']
        from transform.transform1_4 import merge
        df=merge(load_dt)
        print(df)

    task_type=PythonVirtualenvOperator(
        task_id='type.cast',
        python_callable=fun_transform,
        system_site_packages=False,
        requirements="git+https://github.com/test-Esther/transform.git@d3.0.0/transfer1-4"
        trigger_rule='all_done'
            )

#    merge_df=EmptyOperator(
#        task_id='merge.df',
#        trigger_rule='all_done'
#            )

#    de_dup=EmptyOperator(
#        task_id='de.dup',
#        trigger_rule='all_done'
#            )

    start, end=gen_empty('start', 'end')

    start >> task_type >> end
