from datetime import datetime, timedelta
from textwrap import dedent
#from pprint import pprint as pp

from airflow import DAG
import requests

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
    'movie_top_kr',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=5,
    max_active_tasks=10,
    description='movie_top_kr',
   # schedule_interval=timedelta(days=1),
    schedule="10 2 * * *",
    start_date=datetime(2016, 12, 31),
    end_date=datetime(2017, 1, 2),
    catchup=True,
    tags=['movie', 'top10_in_KR'],
) as dag:


    def merge(load_dt):
        import pandas as pd
        read_df = pd.read_parquet('~/tmp/team_parquet')
        
        # 개봉일을 datetime 형식으로 변환
        read_df['openDt'] = pd.to_datetime(read_df['openDt'], format='%Y-%m-%d', errors='coerce')
        cols = [
            'movieNm', #영화명(국문)을 출력합니다.
            'openDt', #영화의 개봉일을 출력합니다.
            'salesAmt', #해당일의 매출액을 출력합니다.
            #'load_dt', # 입수일자
            #'repNationCd', #한국외국영화 유무
                ]
        df = read_df[cols]

        # 2016년 1월부터 12월 사이에 개봉한 영화 필터링
        start_date = '2016-01-01'
        end_date = '2016-12-31'
        df_filtered = df[(df['openDt'] >= start_date) & (df['openDt'] <= end_date)]

        df_filtered['salesAmt'] = pd.to_numeric(df_filtered['salesAmt'], errors='coerce')

        # 중복된 영화명 제거 (가장 높은 매출액을 가진 영화만 남기기)
        df_unique = df_filtered.loc[df_filtered.groupby('movieNm')['salesAmt'].idxmax()]
        
        # 매출액을 기준으로 상위 10개 영화 추출     # 매출액을 숫자형으로 변환
        top10_list = df_unique.sort_values(by='salesAmt', ascending=False).head(10)

        # 결과를 Parquet 파일로 저장
        output_path = "~/tmp/team_jkl/top10_list.parquet"
        top10_list.to_parquet(output_path, index=False)

        print(f"Results have been saved to {output_path}")
        # 한국 영화만 필터링
        #df_korean_movies = df_top10[df_top10['repNationCd'] == 'K']  # 'K'은 한국 영화
        print(top10_list.head(10))

    start = EmptyOperator(
        task_id='start'
        )


    process_movies = PythonVirtualenvOperator(
        task_id='process_movies',
        python_callable=merge,
        requirements=["git+https://github.com/test-Esther/transform"],
        system_site_packages=False,
        op_kwargs={'load_dt': '{{ ds }}'},  # Pass execution date as load_dt
        )
    
    end = EmptyOperator(
        task_id='end'
        )
    
    # Define task dependencies
    start >> process_movies >> end
