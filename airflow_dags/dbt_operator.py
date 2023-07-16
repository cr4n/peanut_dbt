from airflow.operators.bash_operator import BashOperator

DBT_DIR = '/usr/local/airflow/peanut_dbt'

def dbtRun(model, dag):
    return BashOperator(task_id=model,
                bash_command=f'cd {DBT_DIR} && dbt run --models {model} --profiles-dir {DBT_DIR}',
                dag=dag)

def dbtSnapshot(model, dag):
    return BashOperator(task_id=model,
                bash_command=f'cd {DBT_DIR} && dbt snapshot --models {model} --profiles-dir {DBT_DIR}',
                dag=dag)

