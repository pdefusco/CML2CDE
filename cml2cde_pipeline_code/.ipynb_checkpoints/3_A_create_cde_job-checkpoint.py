# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2022
#  All rights reserved.
#
#  Applicable Open Source License: Apache 2.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# ###########################################################################

## Part 3 A - Create CDE Job 

### Required Env Variables ###
# 1. JOBS API URL for CDE Virtual Cluster
# 2. CDE Resource Name
# 3. CDE Job Name

import os
import json
import requests

print("Creating CDE Resource and Job on CDE Virtual Cluster:")
print(os.environ["JOBS_API_URL"])
### Download CDE Token ###

print("\nDownloading CDE Token...\n")

rep = os.environ["JOBS_API_URL"].split("/")[2].split(".")[0]
os.environ["GET_TOKEN_URL"] = os.environ["JOBS_API_URL"].replace(rep, "service").replace("dex/api/v1", "gateway/authtkn/knoxtoken/api/v1/token")
token_json = !curl -u $WORKLOAD_USER:$WORKLOAD_PASSWORD $GET_TOKEN_URL
os.environ["ACCESS_TOKEN"] = json.loads(token_json[5])["access_token"]

tok = json.loads(token_json[5])["access_token"]

print("CDE Token Downloaded\n")

### Create CDE Resource and CDE Job ###

### Create CDE Resource ###

print("Creating CDE Resource...\n")
print(os.environ["CDE_Resource_Name"])

url = os.environ["JOBS_API_URL"] + "/resources"
myobj = {"name": os.environ["CDE_Resource_Name"]}
data_to_send = json.dumps(myobj).encode("utf-8")
headers = {"Authorization": f'Bearer {tok}', 
          "Content-Type": "application/json"}

x = requests.post(url, data=data_to_send, headers=headers)
print(x.status_code)

### Upload Spark Job to CDE Resource ###

print("Uploading Spark Job to CDE Resource...\n")

!curl -H "Authorization: Bearer $ACCESS_TOKEN" -X PUT \
  "$CDE_VC_ENDPOINT/resources/$CDE_Resource_Name/LC_all.py" \
  -F "file=@/home/cdsw/LC_all.py"

### Create CDE Job from CDE Resource ###

print("Creating CDE Job from CDE Resource...\n")
print(os.environ["CDE_Job_Name"])

# Create the CDE Job from the resource
!curl -H "Authorization: Bearer $ACCESS_TOKEN" -X POST "$JOBS_API_URL/jobs" \
          -H "accept: application/json" \
          -H "Content-Type: application/json" \
          -d "{ \"name\": \"cml2cde_cicd_job\", \"type\": \"spark\", \"retentionPolicy\": \"keep_indefinitely\", \"mounts\": [ { \"dirPrefix\": \"/\", \"resourceName\": \"cml2cde_cicd_resource\" } ], \"spark\": { \"file\": \"LC_all.py\"},\"schedule\": { \"enabled\": false} }"