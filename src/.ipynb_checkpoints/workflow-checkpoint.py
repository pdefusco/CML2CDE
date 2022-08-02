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
    
    def ooziexml_to_dict(oozie_workflow_path)
        return workflow_d
    
    def parse_workflow_properties(oozie_workflow_path):
        return properties_d
    
    
    def workflow_properties_lookup(workflow_d, props):
        return workflow_d_props
    
    
    def query_properties_lookup(hive_file_path, hive_file_name, props):
        pass 
    
