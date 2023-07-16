from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from dbt_operator import dbtRun

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 14),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily'
}

dag = DAG(
    'model_users',
    default_args=default_args,
    description='Daily and monthly active users',
    schedule_interval=timedelta(days=1)
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# Dimension table for Users
dim_users = dbtRun('dim_users', dag)
dim_time_zone = dbtRun('dim_time_zone', dag)
dim_profile_stats = dbtRun('dim_profile_stats', dag) 

# Fact tables for daily and monthly active users
fct_user_sessions = dbtRun('fct_user_sessions', dag)
fct_daily_active_users = dbtRun('fct_daily_active_users', dag)
fct_monthly_active_users = dbtRun('fct_monthly_active_users', dag)
agg_daily_active_users = dbtRun('agg_daily_active_users', dag)
agg_monthly_active_users = dbtRun('agg_monthly_active_users', dag)

# Task dependencies
start >> [dim_time_zone, dim_profile_stats, dim_users, fct_user_sessions]
fct_user_sessions >> fct_daily_active_users >> agg_daily_active_users >> end
fct_daily_active_users >> fct_monthly_active_users  >> agg_monthly_active_users >> end
[dim_time_zone, dim_profile_stats, dim_users] >> end