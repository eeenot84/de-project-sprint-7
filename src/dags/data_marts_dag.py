"""
DAG для автоматизации обновления витрин данных.
"""

import pendulum
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

EVENTS_PATH = "/user/master/data/geo/events"
CITIES_PATH = "/user/master/data/geo/cities"
USER_MART_PATH = "/user/master/data/geo/user_mart"
ZONE_MART_PATH = "/user/master/data/geo/zone_mart"
FRIENDS_RECOMMENDATION_MART_PATH = "/user/master/data/geo/friends_recommendation_mart"

SCRIPTS_PATH = os.getenv('SCRIPTS_PATH', '/opt/airflow/scripts')

spark_kwargs = {
    'conf': {"spark.driver.maxResultSize": "20g"},
    'num_executors': 4,
    'executor_memory': "4g",
    'executor_cores': 2,
    'driver_memory': '2g',
}

daily_dag = DAG(
    'daily_data_marts',
    default_args=default_args,
    description='Ежедневное обновление витрин пользователей и рекомендаций друзей',
    schedule_interval='0 2 * * *',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['data-marts', 'geo-analytics', 'daily'],
)

start_daily = DummyOperator(task_id='start', dag=daily_dag)

build_user_mart = SparkSubmitOperator(
    task_id='build_user_mart',
    dag=daily_dag,
    application=f"{SCRIPTS_PATH}/user_mart.py",
    conn_id="yarn_spark",
    application_args=[
        EVENTS_PATH,
        CITIES_PATH,
        USER_MART_PATH,
    ],
    **spark_kwargs
)

build_friends_recommendation_mart = SparkSubmitOperator(
    task_id='build_friends_recommendation_mart',
    dag=daily_dag,
    application=f"{SCRIPTS_PATH}/friends_recommendation_mart.py",
    conn_id="yarn_spark",
    application_args=[
        EVENTS_PATH,
        CITIES_PATH,
        FRIENDS_RECOMMENDATION_MART_PATH,
    ],
    **spark_kwargs
)

finish_daily = DummyOperator(task_id='finish', dag=daily_dag)

start_daily >> build_user_mart >> build_friends_recommendation_mart >> finish_daily

weekly_dag = DAG(
    'weekly_zone_mart',
    default_args=default_args,
    description='Еженедельное обновление витрины зон',
    schedule_interval='0 3 * * 1',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['data-marts', 'geo-analytics', 'weekly'],
)

start_weekly = DummyOperator(task_id='start', dag=weekly_dag)

build_zone_mart = SparkSubmitOperator(
    task_id='build_zone_mart',
    dag=weekly_dag,
    application=f"{SCRIPTS_PATH}/zone_mart.py",
    conn_id="yarn_spark",
    application_args=[
        EVENTS_PATH,
        CITIES_PATH,
        ZONE_MART_PATH,
    ],
    **spark_kwargs
)

finish_weekly = DummyOperator(task_id='finish', dag=weekly_dag)

start_weekly >> build_zone_mart >> finish_weekly

