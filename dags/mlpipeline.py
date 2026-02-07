from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def preprocess_data():
    print("Preprocessing data...")

def train_model():
    print("Training model...")

def evaluate_model():
    print("Evaluating model...")

def deploy_model():
    print("Deploying model...")

with DAG(
    'ml_pipeline',
    start_date=datetime(2026, 2, 7),
    schedule='@weekly',
) as dag:
    preprocess = PythonOperator(task_id='preprocess_data', python_callable=preprocess_data)
    train = PythonOperator(task_id='train_model', python_callable=train_model)
    evaluate = PythonOperator(task_id='evaluate_model', python_callable=evaluate_model)
    deploy = PythonOperator(task_id='deploy_model', python_callable=deploy_model)

    preprocess >> train >> evaluate >> deploy
