# airflow_jkl
- 해당 프로그램은 ETL 과정의 결과를 airflow로 가시화합니다.

## 설치 방법
```
$ pip install git+https://github.com/test-Esther/airflow_jkl.git
```
로 설치할 수 있습니다.

## 환경 설정
- 해당 프로그램 사용 시 airflow가 필수로 설치되어 있어야 합니다.
```
$ pyenv virtualenv <virtual environment> <environment name>
$ pyenv shell <environment name>
$ pip install apache-airflow
```
이후,

```
$ vi ~/.zshrc

[Airflow]
$ export AIRFLOW_HOME=~/airflow_<YOURS>
$ export AIRFLOW__CORE__DAGS_FOLDER=<YOURS>/airflow/dags
$ export AIRLFOW__CORE__LOAD_EXAMPLES=False
```

## 실행 방법
```
$ airflow standalone
```

## 유의사항(key 유무) 등
```
# 해당 프로그램에 접속하려면 한국영화진흥위원회(kobis)에서 발급한 key가 필요하며, ~/.zshrc에 저장해야 합니다. 아래는 예시입니다.
$ export MOVIE_API_KEY=<YOUR KEY>
```

