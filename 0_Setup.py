#
#Copyright (c) 2022 Cloudera, Inc. All rights reserved.
#

### Installing Requirements
!pip3 install -r requirements.txt

import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime
import yaml

#Extracting the correct URL from hive-site.xml
tree = ET.parse('/etc/hadoop/conf/hive-site.xml')
root = tree.getroot()

for prop in root.findall('property'):
    if prop.find('name').text == "hive.metastore.warehouse.dir":
        storage = prop.find('value').text.split("/")[0] + "//" + prop.find('value').text.split("/")[2]

print("The correct Cloud Storage URL is:{}".format(storage))

os.environ['STORAGE'] = storage

### Downloading Lab Files

!curl -O https://www.cloudera.com/content/dam/www/marketing/tutorials/cdp-using-cli-api-to-automate-access-to-cloudera-data-engineering/tutorial-files.zip
!mv tutorial-files.zip /home/cdsw/data
!unzip /home/cdsw/data/tutorial-files.zip -d /home/cdsw/data
  
!hdfs dfs -mkdir -p $STORAGE/datalake/cde-demo
!hdfs dfs -copyFromLocal /home/cdsw/data/PPP-Over-150k-ALL.csv $STORAGE/datalake/cde-demo/PPP-Over-150k-ALL.csv
!hdfs dfs -copyFromLocal /home/cdsw/data/PPP-Sub-150k-TX.csv $STORAGE/datalake/cde-demo/PPP-Sub-150k-TX.csv
!hdfs dfs -ls $STORAGE/datalake/cde-demo

!hdfs dfs -copyFromLocal /home/cdsw/data/LoanStats_2015_subset_071821.csv $STORAGE/datalake/cde-demo/LoanStats_2015_subset_071821.csv

!rm /home/cdsw/data/PPP-Over-150k-ALL.csv /home/cdsw/data/PPP-Sub-150k-TX.csv /home/cdsw/data/config.yaml
!rm /home/cdsw/data/Data_Extraction_Over_150k.py /home/cdsw/data/Data_Extraction_Sub_150k.py
!rm /home/cdsw/data/tutorial-files.zip
!rm /home/cdsw/data/Create_Reports.py


