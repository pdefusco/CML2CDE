import numpy as np
import pandas as pd
import os
from os.path import exists
import json
import sys
import re
import requests
from requests_toolbelt import MultipartEncoder
import xmltodict as xd
import pyparsing


class CdeJob:
    '''Class representing a CDE Job'''
    
    def __init__(self, cde_job_name, cde_resource_name, cde_job_file, workflow_xml_dict):
        self.cde_job_name = cde_job_name
        self.cde_resource = cde_resource_name
        self.cde_job_file = cde_job_file
        self.workflow_xml_dict = workflow_xml_dict
    
    
class SparkCdeJob(CdeJob):
    '''Class representing a Spark CDE Job. Subtype of CDE Job.''' 

    def __init__(self, cde_job_name, cde_resource_name, cde_job_file, workflow_xml_dict):
        self.spark_cde_payload = spark_cde_payload
        super().__init__(cde_job_name, cde_resource_name, cde_job_file, workflow_xml_dict)
    
    
    def oozie_to_cde_spark_payload(cde_resource_name):
        return spark_cde_payload
    
    
class AirflowCdeJob(CdeJob):
    '''Class representing an Airflow CDE Job. Subtype of CDE Job.'''
    
    def __init__(self, cde_job_name, cde_resource_name, cde_job_file, workflow_xml_dict):
        super().__init__(cde_job_name, cde_resource_name, cde_job_file, workflow_xml_dict)
    
    
    def initialize_dag(dag_dir, dag_file_name):
        with open(dag_dir+"/"+dag_file_name, 'w') as f:
            f.write('# The new Airflow DAG')
    
    
    def dag_imports(dag_dir, dag_file_name):
        imports = """\nfrom dateutil import parser
    \nfrom datetime import datetime, timedelta
    \nfrom datetime import timezone
    \nfrom airflow import DAG
    \nfrom airflow.operators.email import EmailOperator
    \nfrom airflow.operators.python_operator import PythonOperator
    \nfrom cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
    \nfrom cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator\n\n"""
    
        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(imports)

    
    def dag_declaration(dag_dir, dag_file_name):
    
        declaration = """default_args = {
    'owner': 'your_username_here',
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'start_date': parser.isoparse('2021-05-25T07:33:37.393Z').replace(tzinfo=timezone.utc)
    }

dag = DAG(
    'airflow-pipeline-demo',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False
    )\n\n"""
    
        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(declaration)
    
    
    def parse_spark_oozie_action(a):

        if "spark" in a.keys():

            task_id = a["spark"]["name"]
            step_name = task_id+"_Step" 
            step_name = step_name.replace('-', '')
            spark_cde_job_name = task_id

            print("Extracted Job Name: {}".format(task_id))

            return task_id, step_name, spark_cde_job_name

        else:
            print("Error. This is not a Spark Oozie Action")
    
    
    def parse_hive_oozie_action(a):
    
        #Checking if this is a Hive Oozie Action
        if "hive" in a.keys():

            #CDE Operator Task ID
            task_id = a['@name']

            #CDW Operator Name
            step_name = task_id+"_Step"
            step_name = step_name.replace('-', '')

            #Parsing SQL from Hive file
            with open(a['hive']['script'], 'r') as f:
                hive_sql = f.read()
                cdw_query = hive_sql.replace("\n", "")

        return task_id, step_name, cdw_query
    
    
    def parse_email_oozie_action(a):
    
        if "email" in a.keys():

            #Task ID
            task_id = a['@name']

            #Operator Name
            step_name = task_id+"_Step"
            step_name = step_name.replace('-', '')

            #Extracting Email Fields

            action = a['email']

            if action.__contains__('to'):
                email_to = a['email']['to'] 
            if action.__contains__('cc'):
                email_cc = a['email']['cc']
            if action.__contains__('subject'):
                email_subject = a['email']['subject']
            if action.__contains__('body'):
                email_body = a['email']['body']

            return task_id, step_name, email_to, email_cc, email_subject, email_body
    
    
    def parse_shell_oozie_action(a):
    
        if "shell" in a.keys():

            #CDE Operator Task ID
            task_id = a['@name']

            #CDW Operator Name
            step_name = task_id+"_Step"
            step_name = step_name.replace('-', '')

            return task_id, step_name
    
    
    def append_cde_spark_operator(dag_dir, dag_file_name, task_id, step_name, spark_cde_job_name):
    
        spark_operator = """{} = CDEJobRunOperator(
        task_id='{}',
        dag=dag,
        job_name='{}'
        )\n\n""".format(step_name, task_id, spark_cde_job_name)

        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(spark_operator)
    
    
    def append_cdw_operator(dag_dir, dag_file_name, task_id, step_name, cdw_query):    
    
        cdw_operator = '''cdw_query = """{}"""

{} = CDWOperator(
    task_id="{}",
    dag=dag,
    cli_conn_id="hive_conn",
    hql=cdw_query,
    schema='default',
    ### CDW related args ###
    use_proxy_user=False,
    query_isolation=True
)\n\n'''.format(cdw_query, step_name, task_id)
    
        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(cdw_operator)
    
    
    def append_email_operator(dag_dir, dag_file_name, task_id, step_name, email_to, email_cc, email_subject, email_body):
    
        email_operator ='''
    {} = EmailOperator( 
    task_id="{}", 
    to="{}", 
    cc="{}",
    subject="{}", 
    html_content="{}", 
    dag=dag)
        '''.format(step_name, task_id, email_to, email_cc, email_subject, email_body)

        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(email_operator)
    
    
    def append_bash_operator(dag_dir, dag_file_name, task_id, step_name):
    
        bash_operator = '''{} = BashOperator(
        task_id="{}",
        bash_command="echo \'here is the message'")'''.format(task_id, step_name)

        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(bash_operator)
    
    
    def append_python_operator(dag_dir, dag_file_name):
    
        print("Action not Found. Replacing Action with Airflow Python Operator Stub")

        task_id = "PythonOperator"
        step_name = "StepStub"

        python_operator = """\ndef my_func():\n\tpass\n 
        {} = PythonOperator(task_id='{}', python_callable=my_func)""".format(step_name, task_id)

        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(python_operator)
    
    
    def oozie_to_cde_airflow_payload(cde_resource_name):
        pass