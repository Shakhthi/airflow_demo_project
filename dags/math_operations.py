from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def start_task(**context):
    context['ti'].xcom_push(key='cur_value', value=4)
    #context['num'].xcom_push(key='cur_value', value=4)
    print("value instantiated...")

def add_task(**context):
    num = context['ti'].xcom_pull(key='cur_value', task_ids='start_task')
    add_result = num + 10
    context['ti'].xcom_push(key='cur_value', value=add_result)
    print(f"Result after addition: {add_result}")

def multiply_task(**context):
    result = context['ti'].xcom_pull(key='cur_value', task_ids='add_task')
    mul_result = result * 2
    context['ti'].xcom_push(key='cur_value', value=mul_result)
    print(f"Result after multiplication: {mul_result}")

def subtract_task(**context):
    result = context['ti'].xcom_pull(key='cur_value', task_ids='multiply_task')
    sub_value = result - 5
    context['ti'].xcom_push(key='cur_value', value=sub_value)
    print(f"Result after subtraction: {sub_value}")

def square_task(**context):
    final_value = context['ti'].xcom_pull(key='cur_value', task_ids='subtract_task')
    square_result = final_value ** 2
    context['ti'].xcom_push(key='square_result', value=square_result)
    print(f'Result after squaring: {square_result}')

with DAG(
    'math_operations',
    start_date=datetime(2026, 2, 7),
    schedule='@daily',
    catchup=False,
) as dag:
    start = PythonOperator(
        task_id='start_task', python_callable=start_task, show_return_value_in_logs=True, do_xcom_push=True)
    
    add = PythonOperator(
        task_id='add_task', python_callable=add_task, show_return_value_in_logs=True)
    
    multiply = PythonOperator(
        task_id='multiply_task', python_callable=multiply_task, show_return_value_in_logs=True)
    
    subtract = PythonOperator(
        task_id='subtract_task', python_callable=subtract_task, show_return_value_in_logs=True)
    
    square = PythonOperator(
        task_id='square_task', python_callable=square_task, show_return_value_in_logs=True)

    start >> add >> multiply >> subtract >> square