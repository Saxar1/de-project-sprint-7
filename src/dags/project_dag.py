import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id = "project7",
    default_args=default_args,
    schedule_interval=None,
)

events_datamart = SparkSubmitOperator(
    task_id='events_datamart',
    dag=dag_spark,
    application ='lessons/event_datamart.py' ,
    conn_id= 'yarn_spark',
    application_args = ["2022-05-25", "/user/master/data/geo/events", "/user/saxarr0k/data/geo/geo_2.csv", "/user/saxarr0k/analytics/user_datamart"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '2g'
)

geolayer_datamart = SparkSubmitOperator(
    task_id='geolayer_datamart',
    dag=dag_spark,
    application ='/lessons/geolayer_datamart.py',
    conn_id= 'yarn_spark',
    application_args = ["2022-05-25", "/user/master/data/geo/events", "/user/saxarr0k/data/geo/geo_2.csv", "/user/saxarr0k/analytics/geolayer_datamart"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '2g'
)

friends_datamart = SparkSubmitOperator(
    task_id='friends_datamart',
    dag=dag_spark,
    application ='/lessons/friends_datamart.py',
    conn_id= 'yarn_spark',
    application_args = ["2022-05-25", "/user/master/data/geo/events", "/user/saxarr0k/data/geo/geo_2.csv", "/user/saxarr0k/analytics/friends_datamart"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '2g'
)

events_datamart >> geolayer_datamart >> friends_datamart