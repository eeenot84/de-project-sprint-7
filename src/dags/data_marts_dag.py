"""
DAG для автоматизации обновления витрин данных.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

EVENTS_PATH = "/user/master/data/geo/events"
CITIES_PATH = "/user/eeenot007/data/geo/cities/geo.csv"
USER_MART_PATH = "/user/master/data/geo/user_mart"
ZONE_MART_PATH = "/user/master/data/geo/zone_mart"
FRIENDS_RECOMMENDATION_MART_PATH = "/user/master/data/geo/friends_recommendation_mart"

SCRIPTS_PATH = os.getenv('SCRIPTS_PATH', '/opt/airflow/scripts')

daily_dag = DAG(
    'daily_data_marts',
    default_args=default_args,
    description='Ежедневное обновление витрин пользователей и рекомендаций друзей',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data-marts', 'geo-analytics', 'daily'],
)
build_user_mart = BashOperator(
    task_id='build_user_mart',
    bash_command=f"""
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 2g \
        --executor-memory 4g \
        --executor-cores 2 \
        --num-executors 4 \
        {SCRIPTS_PATH}/user_mart.py \
        {EVENTS_PATH} \
        {CITIES_PATH} \
        {USER_MART_PATH}
    """,
    dag=daily_dag,
)

build_friends_recommendation_mart = BashOperator(
    task_id='build_friends_recommendation_mart',
    bash_command=f"""
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 2g \
        --executor-memory 4g \
        --executor-cores 2 \
        --num-executors 4 \
        {SCRIPTS_PATH}/friends_recommendation_mart.py \
        {EVENTS_PATH} \
        {CITIES_PATH} \
        {FRIENDS_RECOMMENDATION_MART_PATH}
    """,
    dag=daily_dag,
)

build_user_mart >> build_friends_recommendation_mart

weekly_dag = DAG(
    'weekly_zone_mart',
    default_args=default_args,
    description='Еженедельное обновление витрины зон',
    schedule_interval='0 3 * * 1',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data-marts', 'geo-analytics', 'weekly'],
)
build_zone_mart = BashOperator(
    task_id='build_zone_mart',
    bash_command=f"""
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 2g \
        --executor-memory 4g \
        --executor-cores 2 \
        --num-executors 4 \
        {SCRIPTS_PATH}/zone_mart.py \
        {EVENTS_PATH} \
        {CITIES_PATH} \
        {ZONE_MART_PATH}
    """,
    dag=weekly_dag,
)

