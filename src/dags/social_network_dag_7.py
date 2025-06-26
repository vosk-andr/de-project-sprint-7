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
dag_id = "social_network",
default_args=default_args,
schedule_interval=None,
)

event_sity = SparkSubmitOperator(
task_id='event_sity',
dag=dag_spark,
application ='/lessons/event_sity.py' ,
conn_id= 'yarn_spark',
application_args = ["/user/master/data/geo/events", "/user/reydanyk/project_7/stg/geo.csv", '/user/reydanyk/project_7/mart/event_sity', '0.05', '42'],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

user_city = SparkSubmitOperator(
task_id='user_city',
dag=dag_spark,
application ='/lessons/user_city.py' ,
conn_id= 'yarn_spark',
application_args = ["/user/reydanyk/project_7/mart/event_sity", "/user/reydanyk/project_7/mart/user_sity"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

zone_analytics = SparkSubmitOperator(
task_id='zone_analytics',
dag=dag_spark,
application ='/lessons/zone_analytics.py' ,
conn_id= 'yarn_spark',
application_args = ["/user/reydanyk/project_7/mart/user_sity", "/user/reydanyk/project_7/analytics/zone_analytics"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

recommendations = SparkSubmitOperator(
task_id='recommendations',
dag=dag_spark,
application ='/lessons/recommendations.py' ,
conn_id= 'yarn_spark',
application_args = ['/user/reydanyk/project_7/mart/event_sity', "/user/reydanyk/project_7/mart/user_sity", "/user/reydanyk/project_7/analytics/recommendations"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)


event_sity >> user_city >> [zone_analytics, recommendations]