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


class CdeConnection:
    '''Class to establish a connection to a CDE Virtual Cluster
       and interact with it e.g. upload Spark CDE Job files'''
    
    def __init__(self, JOBS_API_URL, WORKLOAD_USER):
        self.JOBS_API_URL = JOBS_API_URL
        self.WORKLOAD_USER = WORKLOAD_USER
        
    # Set user token to interact with CDE Service remotely
    def set_cde_token(self, WORKLOAD_PASSWORD):

        os.environ["JOBS_API_URL"] = self.JOBS_API_URL
        os.environ["WORKLOAD_USER"] = self.WORKLOAD_USER

        rep = os.environ["JOBS_API_URL"].split("/")[2].split(".")[0]
        os.environ["GET_TOKEN_URL"] = os.environ["JOBS_API_URL"].replace(rep, "service").replace("dex/api/v1", "gateway/authtkn/knoxtoken/api/v1/token")

        token_json = requests.get(os.environ["GET_TOKEN_URL"], auth=(os.environ["WORKLOAD_USER"], WORKLOAD_PASSWORD))

        return json.loads(token_json.text)["access_token"]
    
    # Create CDE Resource to upload Spark CDE Job files
    def create_cde_resource(self, token, resource_name):

        print("Started Creating Resource {}".format(resource_name))
        
        url = os.environ["JOBS_API_URL"] + "/resources"
        myobj = {"name": str(resource_name)}
        data_to_send = json.dumps(myobj).encode("utf-8")

        headers = {
            'Authorization': f"Bearer {token}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        x = requests.post(url, data=data_to_send, headers=headers)

        if x.status_code == 201:
            print("Creating Resource {} has Succeeded".format(resource_name))
        else:
            print(x.status_code)
            print(x.text)
        

    #Upload Spark CDE Job file to CDE Resource
    def upload_file(self, resource_name, job_path, file_name, token):

        print("Uploading File {0} to CDE Resource {1}".format(file_name, resource_name))

        m = MultipartEncoder(
            fields={
                    'file': ('filename', open(job_path+"/"+file_name, 'rb'), 'text/plain')}
            )

        PUT = '{jobs_api_url}/resources/{resource_name}/{file_name}'.format(jobs_api_url=os.environ["JOBS_API_URL"], resource_name=resource_name, file_name=file_name)
        
        x = requests.put(PUT, data=m, headers={'Authorization': f"Bearer {token}",'Content-Type': m.content_type})
        print("Response Status Code {}".format(x.status_code))
        
        if x.status_code == 201:
            print("Uploading File {0} to CDE Resource {1} has Succeeded".format(file_name, resource_name))
        else:
            print(x.status_code)
            print(x.text)
        
     
    def create_spark_job_from_resource(self, token, cde_job_name, CDE_RESOURCE_NAME, PYSPARK_EXAMPE_SCRIPT_NAME, spark_confs={"spark.pyspark.python": "python3"}):

        print("Started Creating CDE Spark Job {0} with Script {1}".format(cde_job_name, PYSPARK_EXAMPE_SCRIPT_NAME))
         
        ### Any Spark Job Configuration Options (Not Mandatory) ###
        #spark_confs_example = { 
                  #"spark.dynamicAllocation.maxExecutors": "6",
                  #"spark.dynamicAllocation.minExecutors": "2",
                  #"spark.executor.extraJavaOptions": "-Dsun.security.krb5.debug=true -Dsun.security.spnego.debug=true",
                  #"spark.hadoop.fs.s3a.metadatastore.impl": "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore",
                  #"spark.kubernetes.memoryOverheadFactor": "0.2",
                  #"spark.pyspark.python": "python3"
                  #"spark.rpc.askTimeout": "600",
                  #"spark.sql.shuffle.partitions": "48",
                  #"spark.yarn.access.hadoopFileSystems": "s3a://your_data_lake_here"
                #}      
        
        cde_payload = {
              "name": cde_job_name,# CDE Job Name As you want it to appear in the CDE JOBS UI
              "type": "spark",
              "retentionPolicy": "keep_indefinitely",
              "mounts": [
                {
                  "resourceName": CDE_RESOURCE_NAME
                }
              ],
              "spark": {
                "file": PYSPARK_EXAMPE_SCRIPT_NAME,
                "driverMemory": "1g",
                "driverCores": 1, #this must be an integer
                "executorMemory": "4g",
                "executorCores": 1, #this must be an integer
                "conf": spark_confs,
                "logLevel": "INFO"
              },
              "schedule": {
                "enabled": False,
                "user": os.environ["WORKLOAD_USER"] #Your CDP Workload User is automatically set by CML as an Environment Variable
              }
            }
        
        headers = {
            'Authorization': f"Bearer {token}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        PUT = '{}/jobs'.format(os.environ["JOBS_API_URL"])

        data = json.dumps(cde_payload)

        x = requests.post(PUT, headers=headers, data=data)

        if x.status_code == 201:
            print("Creating CDE Spark Job {0} with Script {1} has Succeeded".format(cde_job_name, PYSPARK_EXAMPE_SCRIPT_NAME))
        else:
            print(x.status_code)
            print(x.text)
           
    def run_spark_job(self, token, cde_job_name, driver_cores = 2, driver_memory = "4g", executor_cores = 4, executor_memory = "4g", num_executors = 4):
    
        print("Started to Submit Spark Job {}".format(cde_job_name))

        cde_payload = {"overrides": 
                       {"spark":
                        {"driverCores": driver_cores, 
                         "driverMemory": driver_memory, 
                         "executorCores": executor_cores, 
                         "executorMemory": executor_memory, 
                         "numExecutors": num_executors}
                       }
                      }      
              
        headers = {
            'Authorization': f"Bearer {token}",
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        POST = "{}/jobs/".format(os.environ["JOBS_API_URL"])+cde_job_name+"/run"

        data = json.dumps(cde_payload)

        x = requests.post(POST, headers=headers, data=data)

        if x.status_code == 201:
            print("Submitting CDE Spark Job {} has Succeeded".format(cde_job_name))
            print("This doesn't necessarily mean that the CDE Spark Job has Succeeded")
            print("Please visit the CDE Job Runs UI to check on CDE Job Status")
        else:
            print(x.status_code)
            print(x.text)