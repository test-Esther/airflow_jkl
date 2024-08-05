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
    'Extract1-4',
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
    start_date=datetime(2016, 1, 1),
    end_date=datetime(2016, 1, 3),
    catchup=True,
    tags=['movie', '2016', 'extract9_12'],
) as dag:

    def pic():
        from extract.icebreaking import pic
        pic()

    def branch_fun(ds_nodash):
        import os
        home_dir=os.path.expanduser('~')
        path=os.path.join(home_dir, f"~/tmp/team_parquet/")
        if os.path.exists(path):
            return "rm.dir"
        else:
            return "get.data"
        
    branch_op = BranchPythonOperator(
        task_id="branch.op",
	    python_callable=branch_fun,
        
        ) 

    def get_data(ds_nodash):
        from extract.extract_1_4 import save2df
        df=save2df(ds_nodash)
        print(df.head(5))

    get_data = PythonVirtualenvOperator(
        task_id="get.data",
        python_callable=get_data,
        requirements=["git+https://github.com/test-Esther/extract.git@release/d2.0.0"],
        system_site_packages=False,
        trigger_rule="one_success"
        )

    def save_data(ds_nodash):
        from extract.extract_1_4 import apply_type2df
        df=apply_type2df(load_dt=ds_nodash)
        print(df.head(10))
        g=df.groupby('openDt')
        sum_df=g.agg({'audiCnt': 'sum'}).reset_index()
        print(sum_df)

    save_data = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=save_data,
        requirements=["git+https://github.com/test-Esther/extract.git@release/d2.0.0"],
        system_site_packages=False,
        trigger_rule="one_success"
        )

    start =EmptyOperator(
        task_id="start",
                )

    end = EmptyOperator(
        task_id="end",
                )
    

    rm_dir = BashOperator(
	    task_id='rm.dir',
	    bash_command='rm -rf ~/tmp/team_parquet/'
        )

    #get_start = EmptyOperator(task_id='get.start', trigger_rule="all_done")
    #get_end = EmptyOperator(task_id='get.end')


    start >> branch_op

    branch_op >> rm_dir >> get_data
    branch_op >> get_data >> save_data 
    
    save_data >> end
    
