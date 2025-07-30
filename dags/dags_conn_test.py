import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum


with DAG(
    dag_id="dags_conn_test",
    schedule=None, 
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "example2", "airflow-study"],
) as dag:

    run_t1 = EmptyOperator(
        task_id="bash_t1",
    )

    run_t2 = EmptyOperator(
        task_id="bash_t2",
    )

    run_t3 = EmptyOperator(
        task_id="bash_t23",
    )

    run_t4 = EmptyOperator(
        task_id="bash_t4",
    )

    run_t5 = EmptyOperator(
        task_id="bash_t5",
    )

    run_t6 = EmptyOperator(
        task_id="bash_t6",
    )

    run_t7 = EmptyOperator(
        task_id="bash_t7",
    )

    run_t8 = EmptyOperator(
        task_id="bash_t8",
    )

    run_t1 >> [run_t2, run_t3] >> run_t4
    run_t5 >> run_t4
    [run_t4, run_t7] >> run_t6
    run_t6 >> run_t8