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
    start_date=datetime(2016, 5, 1),
    end_date=datetime(2016, 5, 3),
    catchup=True,
    tags=['project', 'movie', '2016'],
) as dag:

    def get_data(ds_nodash):
        #from extract.extract_5_8 import save2df
        #print(df.head(5))
        print("#" * 123)

    get_data = PythonVirtualenvOperator(
            task_id='get.data',
            python_callable=get_data,
            requirements=["git+https://github.com/test-Esther/extract.git@d2.0.0/extract5-8"],
            system_site_packages=False,
            trigger_rule="one_success"
            )


    def save_data(ds_nodash):
        from extract.extract_5_8 import apply_type2df
        df=apply_type2df(load_dt=ds_nodash)

    save_data = PythonVirtualenvOperator(
            task_id='save.data',
            python_callable=save_data,
            system_site_packages=False,
            trigger_rule="one_success",
            requirements=["git+https://github.com/test-Esther/extract.git@d2.0.0/extract5-8"],
            )

#     def branch_fun(ds_nodash):
#        import os
#        home_dir = os.path.expanduser("~")
#        path = os.path.join(home_dir, f"/tmp/team_parquet/load_dt={ds_nodash}")
#        if os.path.exists(path):
#            return "rm.dir"
#        else:
#            return "get.start"

#    branch_op = BranchPythonOperator(
#            task_id='branch.op',
#            python_callable=branch_fun
#            )

    rm_dir = BashOperator(
            task_id='rm.dir',
            bash_command='rm -rf ~/tmp/team_parquet/load_dt={{ ds_nodash }}'
            )

    start = EmptyOperator(
        task_id="start",
        )

    end = EmptyOperator(
        task_id="end",
        )
    
    get_start = EmptyOperator(task_id='get.start', trigger_rule="all_done")
    get_end = EmptyOperator(task_id='get.end')

    start >> rm_dir

    rm_dir >> get_start >> get_data
    
    
    get_data >> get_end >> save_data >> end
    

