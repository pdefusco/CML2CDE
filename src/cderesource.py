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


class CdeResource:
    '''Class to establish a connection to a CDE Virtual Cluster
       and interact with it e.g. upload Spark CDE Job files'''
    
    def __init__(self, JOBS_API_URL, WORKLOAD_USER, cde_resource_name):
        self.JOBS_API_URL = JOBS_API_URL
        self.WORKLOAD_USER = WORKLOAD_USER
        self.cde_resource_name = cde_resource_name
        
        
    # Set user token to interact with CDE Service remotely
    def set_cde_token():
        rep = os.environ["JOBS_API_URL"].split("/")[2].split(".")[0]
        os.environ["GET_TOKEN_URL"] = os.environ["JOBS_API_URL"].replace(rep, "service").replace("dex/api/v1", "gateway/authtkn/knoxtoken/api/v1/token")
        token_json = !curl -u $WORKLOAD_USER:$WORKLOAD_PASSWORD $GET_TOKEN_URL
        os.environ["ACCESS_TOKEN"] = json.loads(token_json[5])["access_token"]

        return json.loads(token_json[5])["access_token"]
    
    
    #Upload Spark CDE Job file to CDE Resource
    def put_file(resource_name, job_path, tok):

        print("Working on Job: {}".format(job_path.split("/")[-1].split(".")[0]))

        m = MultipartEncoder(
            fields={
                    'file': ('filename', open(job_path, 'rb'), 'text/plain')}
            )

        PUT = '{jobs_api_url}/resources/{resource_name}/{file_name}'.format(jobs_api_url=os.environ["JOBS_API_URL"], resource_name=resource_name, file_name=job_path.split("/")[-1])
        
        x = requests.put(PUT, data=m, headers={'Authorization': f"Bearer {tok}",'Content-Type': m.content_type})

        print("Response Status Code {}".format(x.status_code))
        print(x.text)
    
    
    def create_job_from_resource(resource_name, job_path, tok, cde_payload):

        print("Working on Job: {}".format(job_path.split("/")[-1].split(".")[0]))

        headers = {
        'Authorization': f"Bearer {tok}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
        }

        PUT = '{}/jobs'.format(os.environ["JOBS_API_URL"])

        data = json.dumps(cde_payload)

        x = requests.post(PUT, headers=headers, data=data)

        print("Response Status Code {}".format(x.status_code))
        print(x.text)
        print("\n")