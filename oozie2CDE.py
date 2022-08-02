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


class OozieWorkflow:
    '''Class representing the Oozie Workflow we want to convert
       The Oozie workflow is an xml file optionally accompanied by other files such as properties, hive sql syntax, etc.
       Assumes the user has added all files to a dedicated local dir
       '''
    
    def __init__(self, project_path):
        self.project_path = project_path
        self.workflow_xml = workflow_xml
        self.workflow_xml_dict = workflow_xml_dict

    
    def ooziexml_to_dict(oozie_workflow_path)
        pass
    
    
    def parse_workflow_properties(oozie_workflow_path):
        pass
    
    
    def workflow_properties_lookup(d, props):
        pass
    
    
    def query_properties_lookup(hive_file_path, hive_file, props):
        pass
    

class CdeJob:
    '''Class representing a CDE Job'''
    
    def __init__(self, cde_job_name, cde_resource_name, cde_job_file):
        self.cde_job_name = cde_job_name
        self.cde_resource = cde_resource_name
        self.cde_job_file = cde_job_file
    
class SparkCdeJob(CdeJob):
    '''Class representing a Spark CDE Job. Subtype of CDE Job.''' 

    def __init__(self, cde_job_name, cde_resource_name, cde_job_file):
        self.spark_cde_payload = spark_cde_payload
        super().__init__(cde_job_name, cde_resource_name, cde_job_file)
    
    def oozie_to_cde_spark_payload(cde_resource_name):
        pass
    
    
class AirflowCdeJob(CdeJob):
    '''Class representing an Airflow CDE Job. Subtype of CDE Job.'''
    
    def __init__(self, cde_job_name, cde_resource_name, cde_job_file):
        super().__init__(cde_job_name, cde_resource_name, cde_job_file)
    
    def oozie_to_cde_spark_payload(cde_resource_name):
        pass
    
    
class airflow_cde_job:
    '''Class representing an Airflow CDE Job'''
    
    def __init__(self, cde_job_name, cde_resource_name, cde_job_file):
        super().__init__(cde_job_name, cde_resource_name, cde_job_file)
    
    def oozie_to_cde_airflow_payload(cde_resource_name): 
        pass
        
        
class ConversionFactory:
    '''Factory Class used to produce instances of CDE Spark & Airflow Jobs'''
    
    def __init__(self, workflow_xml_dict):
        self.workflow_xml_dict = workflow_xml_dict
        
    
    def initialize_dag():
        pass
    
    
    def dag_imports():
        pass
    
    
    def dag_declaration():
        pass
    
    
    def parse_spark_oozie_action():
        pass
    
    
    def parse_hive_oozie_action():
        pass
    
    
    def parse_email_oozie_action():
        pass
    
    
    def parse_shell_oozie_action():
        pass
    
    
    def append_cde_spark_operator():
        pass
    
    
    def append_cdw_operator():
        pass
    
    
    def append_email_operator():
        pass
    
    
    def append_bash_operator():
        pass
    
    
    def append_python_operator():
        pass
    

class CdeResource:
    '''Class to establish a connection to a CDE Virtual Cluster
       and interact with it e.g. upload Spark CDE Job files'''
    
    def __init__(self, JOBS_API_URL, WORKLOAD_USER, cde_resource_name):
        self.JOBS_API_URL = JOBS_API_URL
        self.WORKLOAD_USER = WORKLOAD_USER
        self.cde_resource_name = cde_resource_name
        
        
    def set_token():
        pass
    
    
    def put_file(cde_resource_name, job_local_path, token):
        pass
    
    
    def create_job_from_resource(resource_name, job_path, tok, cde_payload):
        pass