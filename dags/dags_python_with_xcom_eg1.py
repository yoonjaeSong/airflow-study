from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "example2", "airflow-study"],
) as dag:

    @task(task_id="python_xcom_push_task1")
    def xcom_push1(**kwargs):
        ti = kwargs["ti"]
        ti.xcom_push(key="result1", value="value_1")
        ti.xcom_push(key="result2", value=[1, 2, 3])

    @task(task_id="python_xcom_push_task2")
    def xcom_push2(**kwargs):
        ti = kwargs["ti"]
        ti.xcom_push(key="result1", value="value_2")
        ti.xcom_push(key="result2", value=[1, 2, 3, 4])

    @task(task_id="python_xcom_pull_task")
    def xcom_pull(**kwargs):
        ti = kwargs["ti"]
        value1 = ti.xcom_pull(key='result1', task_idㄴ='python_xcom_push_task1')
        value2 = ti.xcom_pull(key='result2', task_idㄴ='python_xcom_push_task2')
        
        print(value1)
        print(value2)
        
    xcom_push1() >> xcom_push2() >> xcom_pull()