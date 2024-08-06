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
    'movie_extract9_12',
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
    schedule="30 1 * * *",
    start_date=datetime(2016, 9, 1),
    end_date=datetime(2017, 1, 1),
    catchup=True,
    tags=['movie', '2016', 'extract9_12'],
) as dag:
    
    #def pic():
    #    from extract.icebreaking import pic
    #   pic()

    #def branch_fun(ds_nodash):
    #   import os
    #    home_dir = os.path.expanduser("~")
     #   path = os.path.join(home_dir, f"tmp/team_parquet/load_dt={ds_nodash}")
      #  if os.path.exists(path):
       #     return rm_dir.task_id
       # else:
       #     return "get.data"


    def get_data(ds_nodash):
        from extract.extract_9_12 import save2df
        df = save2df(ds_nodash)
        print(df.head(5))


    def fun_divide(ds_nodash, url_param):
        from extract.extract_9_12 import save2df
        df = save2df(load_dt=ds_nodash, url_param=url_param)
        print(df[['movieCd', 'movieNm']].head(5))

        for k, v in url_param.items():
            df[k] = v

        p_cols = ['load_dt'] + list(url_param.keys())
        df.to_parquet('~/tmp/team_parquet', partition_cols=p_cols)


    def save_data(ds_nodash):
        from extract.extract_9_12 import apply_type2df
        df = apply_type2df(load_dt=ds_nodash)
        
        print(df.head(2))
        # 개봉일 기준 그룹핑 누적 관객수 합
        #g = df.groupby('movieNm')
        #sum_df = g.agg({'salesAcc': 'sum'}).reset_index()
        #print(sum_df)
        # 누적매출액 합 기준으로 순위 정렬
        #g = df.groupby('movieNm').agg({'salesAcc': 'sum'}).reset_index()
        #sorted_df = g.sort_values(by='salesAcc', ascending=False)
        #print(sorted_df)


    #branch_op = BranchPythonOperator(
     #   task_id="branch.op",
	  #  python_callable=branch_fun,
        #requirements=[pic_require],
       # ) 


    get_data = PythonVirtualenvOperator(
        task_id="get.data",
        python_callable=get_data,
        requirements=["git+https://github.com/test-Esther/extract@d2.0.0/extract9_12"],
        system_site_packages=False,
        trigger_rule="one_success"
        )

    save_data = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=save_data,
        requirements=["git+https://github.com/test-Esther/extract@d2.0.0/extract9_12"],
        system_site_packages=False,
        )

    start = EmptyOperator(
        task_id="start",
        )

    end = EmptyOperator(
        task_id="end",
        )
    

    rm_dir = BashOperator(
	    task_id='rm.dir',
	    bash_command="rm -rf ~/tmp/team_parquet/load_dt={{ds_nodash}}"
        )

    #get_start = EmptyOperator(task_id='get.start', trigger_rule="all_done")
    #get_end = EmptyOperator(task_id='get.end')


    start >> rm_dir >> get_data >> save_data >> end

    
