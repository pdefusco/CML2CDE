# The new Airflow DAG
from dateutil import parser
    
from datetime import datetime, timedelta
    
import pendulum
    
from airflow import DAG
    
from airflow.operators.email import EmailOperator
    
from airflow.operators.python_operator import PythonOperator
    
from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
    
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

default_args = {
        'owner': 'pauldefusco',
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2016, 1, 1, tz="Europe/Amsterdam")
    }

oozie_2_airflow_dag = DAG(
    'airflow-pipeline-demo',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False
    )

SparkPi_Step = CDEJobRunOperator(
        task_id='SparkPi',
        dag=oozie_2_airflow_dag,
        job_name='SparkPi'
        )

