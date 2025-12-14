import pendulum
import os

from airflow import DAG  # type: ignore
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator  # type: ignore
from airflow.operators.dummy import DummyOperator  # type: ignore

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

args = {
    "owner": "data-engineer",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

kwargs = {
    'conf': {"spark.driver.maxResultSize": "20g"},
    'num_executors': 4,
    'executor_memory': "4g",
    'executor_cores': 2,
    'driver_memory': '2g',
}

exec_date = '{{ ds }}'

EVENTS_PATH = '/user/master/data/geo/events'
CITIES_PATH = '/user/master/data/geo/cities/geo.csv'
USER_MART_PATH = '/user/master/data/geo/user_mart'
ZONE_MART_PATH = '/user/master/data/geo/zone_mart'
FRIENDS_RECOMMENDATION_MART_PATH = '/user/master/data/geo/friends_recommendation_mart'

with DAG(
    'datalake_dag',
    default_args=args,
    description='',
    catchup=False,
    schedule='0 2 * * *',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    tags=['pyspark', 'hadoop', 'hdfs', 'datalake', 'geo', 'datamart'],
    is_paused_upon_creation=True,
) as dag:
    start = DummyOperator(task_id='start')

    build_user_mart = SparkSubmitOperator(
        task_id="user_geo_stats",
        dag=dag,
        application="/opt/airflow/scripts/build_user_mart.py",
        conn_id="yarn_spark",
        application_args=[
            exec_date,
            EVENTS_PATH,
            CITIES_PATH,
            USER_MART_PATH,
        ],
        **kwargs
    )

    build_zone_mart = SparkSubmitOperator(
        task_id="geo_stats",
        dag=dag,
        application="/opt/airflow/scripts/build_zone_mart.py",
        conn_id="yarn_spark",
        application_args=[
            exec_date,
            EVENTS_PATH,
            CITIES_PATH,
            ZONE_MART_PATH,
        ],
        **kwargs
    )

    build_user_recoms = SparkSubmitOperator(
        task_id="user_recommendations",
        dag=dag,
        application="/opt/airflow/scripts/build_friends_recommendation_mart.py",
        conn_id="yarn_spark",
        application_args=[
            exec_date,
            EVENTS_PATH,
            CITIES_PATH,
            FRIENDS_RECOMMENDATION_MART_PATH,
        ],
        **kwargs
    )

    finish = DummyOperator(task_id='finish')

    (
        start
        >> [build_user_mart, build_zone_mart, build_user_recoms]
        >> finish
    )



