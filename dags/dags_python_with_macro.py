from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "example2", "airflow-study"],
) as dag:

    @task(
        task_id="task_using_macro",
        templates_dict={
            'start_date': '{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
            'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(day=-1)) | ds }}'
        }
    )
    def get_datetime_with_macro(**kwargs):
        # template_dict에서 전달받은 변수들 확인 및 출력
        templates_dict = kwargs.get('templates_dict', {})
        print(f"start_date: {templates_dict.get('start_date') or 'start_date가 없습니다.'}")
        print(f"end_date: {templates_dict.get('end_date') or 'end_date가 없습니다.'}")
        


    @task(task_id="task_direct_calc")
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta

        # data_interval_end를 context에서 가져옴
        data_interval_end = kwargs.get('data_interval_end')
        if data_interval_end is None:
            print("data_interval_end가 없습니다.")
            return
        
        data_interval_end.in_timezone("Asia/Seoul")

        # start_date: 전달받은 data_interval_end의 한 달 전의 1일
        start_date = data_interval_end + relativedelta(months=-1, day=1)

        # end_date: 전달받은 data_interval_end의 이전 달 마지막 날
        end_date = data_interval_end.replace(day=1) + relativedelta(day=-1)

        print(f"start_date: {start_date.date()}")
        print(f"end_date: {end_date.date()}")
        
    get_datetime_with_macro() >> get_datetime_calc()