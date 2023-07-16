from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from dbt_operator import dbtRun

default_args = {
    'start_date': datetime(2023, 7, 14),
    'schedule_interval': '0 0 * * *',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
}

dag = DAG(
    'model_content',
    default_args=default_args,
    description='Posts and comments (user generated content)',
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

# External sensor 10 seconds interval check 
ext_dimensions = ExternalTaskSensor(
    task_id='ext_dimensions',
    external_dag_id='model_users',
    external_task_id='dimensions_finished',
    dag=dag,
    poke_interval=10
)

ext_fct_daily_active_users = ExternalTaskSensor(
    task_id='ext_fct_daily_active_users',
    external_dag_id='model_users',
    external_task_id='fct_daily_active_users',
    dag=dag,
    poke_interval=10
)

# Content facts
fct_posts = dbtRun('fct_posts', dag)
fct_comments = dbtRun('fct_comments', dag)

# Aggregates for posts and comments
agg_content_generators = dbtRun('agg_content_generators', dag)
agg_created_posts = dbtRun('agg_created_posts', dag)
agg_first_post = dbtRun('agg_first_post', dag)
agg_hourly_post_creation = dbtRun('agg_hourly_post_creation', dag)

# Task dependencies
start >> [fct_comments, fct_posts, ext_dimensions, ext_fct_daily_active_users] 
[fct_posts, ext_dimensions]  >> agg_first_post 
[fct_posts, ext_dimensions]  >> agg_created_posts 
[fct_posts, ext_fct_daily_active_users] >> agg_content_generators 
[fct_comments, agg_first_post, agg_created_posts, agg_hourly_post_creation, agg_content_generators] >> end