from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.decorators import task
import pendulum

from common.common_func import regist


with DAG(
    dag_id="dags_python_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "example2", "airflow-study"],
) as dag:

    def python_fuc1(start_date, end_date, **kwargs):
        print(f'start_date: {start_date}\nend_date: {end_date}')

    python_t1 = PythonOperator(
        task_id='python_t1',
        python_callable=python_fuc1,
        op_kwargs={'start_date': '{{date_interval_start | ds}}', 'end_date': '{{date_interval_end | ds}}'}
    )

    @task(task_id='python_t2')
    def python_fuc2(**kwargs):
        print(kwargs)
        print(f'ds: {kwargs["ds"]}')
        print(f'ts: {kwargs["ts"]}')
        print(f'data_interval_start: {str(kwargs["data_interval_start"])}')
        print(f'data_interval_end: {str(kwargs["data_interval_end"])}')
        print(f'task_instance: {str(kwargs["ti"])}')

    python_t1 >> python_fuc2()