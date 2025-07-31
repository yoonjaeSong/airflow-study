import pprint
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

from airflow.sdk import dag, task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * *", 
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "example2", "airflow-study"],
) as dag:

    @task(task_id="python_task_1")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return "Whatever you return gets printed in the logs"

    run_this = print_context('task_decorator')
 