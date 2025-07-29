from airflow import DAG
from airflow.providers.python.operators.python import PythonOperator
import pendulum
import random

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",  # 매일 0시 0분에 실행
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "python", "airflow-study"],
) as dag:

    def select_fruit():
        fruit = ["APPLE", "BANANA", "ORAGNE", "AVOCADO"]
        rand_int = random.randint(0, 3)
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id="py_t1",
        python_callable=select_fruit,
    )

    py_t1
