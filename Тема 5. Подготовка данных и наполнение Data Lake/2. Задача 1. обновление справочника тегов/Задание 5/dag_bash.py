import os
import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'owner': 's24268544',
    'start_date': dt.datetime(2022, 5, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    'partition_events',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    partitioning = BashOperator(
        task_id='partitioning',
        bash_command=(
            'spark-submit --master yarn --deploy-mode cluster '
            '/lessons/partition.py 2022-05-31 '
            '/user/master/data/events /user/s24268544/data/events'
        ),
    )

    verified_tags_candidates_d7 = SparkSubmitOperator(
        task_id='verified_tags_candidates_d7',
        application='/lessons/verified_tags_candidates.py',
        conn_id='yarn_spark',
        application_args=[
            '2022-05-31', '7', '100',
            '/user/s24268544/data/events',
            '/user/master/data/snapshots/tags_verified/actual',
            '/user/s24268544/data/analytics/verified_tags_candidates_d7',
        ],
    )

    verified_tags_candidates_d84 = SparkSubmitOperator(
        task_id='verified_tags_candidates_d84',
        application='/lessons/verified_tags_candidates.py',
        conn_id='yarn_spark',
        application_args=[
            '2022-05-31', '84', '100',
            '/user/s24268544/data/events',
            '/user/master/data/snapshots/tags_verified/actual',
            '/user/s24268544/data/analytics/verified_tags_candidates_d84',
        ],
    )

    partitioning >> [verified_tags_candidates_d7, verified_tags_candidates_d84]
