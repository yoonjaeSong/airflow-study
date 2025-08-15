from airflow import DAG
from airflow.providers.standard.operators.branch import BaseBranchOperator
import pendulum
from airflow.decorators import task
from airflow.providers.standard.operators.python import PythonOperator


with DAG(
    dag_id="dags_branch_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    catchup=False,  # start_date에서 설정한 이전 값을 실행할지 안할지 결정하는 변수 True - 이전 값도 실행
    tags=["example", "example2", "airflow-study"],
) as dag:

    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            import random
            print(context)
            
            item_list = ['A', 'B', 'C']
            selected_item = random.choice(item_list)
            
            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B', 'C']:
                return ['task_b', 'task_c']

    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task')
    
    def common_func(**kwargs):
        print(kwargs['selected'])
        
    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected': 'A'}
    )
    
    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected': 'B'}
    )
    
    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected': 'C'}
    )
    
    custom_branch_operator >> [task_a, task_b, task_c]