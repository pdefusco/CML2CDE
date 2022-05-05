# CDE CLI in CML

This project is an entry level tutorial for CDE.

The CDE CLI commands are based on [this tutorial](https://www.cloudera.com/tutorials/cdp-using-cli-api-to-automate-access-to-cloudera-data-engineering.html) by the Cloudera Marketing Team, which additionally contains an example of the CDE REST API.



## Project Overview

The project includes three sections:

1. Creating and scheduling a simple Spark Job via the Cloudera Data Engineering Experience ([CDE](https://docs.cloudera.com/data-engineering/cloud/index.html))
2. Creating and scheduling an Airflow Job via CDE
3. Creating and scheduling Spark Jobs via the CDE CLI from Cloudera Machine Learning ([CML](https://docs.cloudera.com/machine-learning/cloud/index.html))



#### Section 1 - Creating and scheduling a simple Spark Job

Log into the CDE experience and create a new resource from the "Resources" tab. Please pick a unique name. 

A resource allows you to upload files and dependencies for reuse. This makes managing spark-submits easier.

Upload the files located in the "manual_jobs" directory of this project in your resource. 


Next, we will create three jobs with the following settings. For each of these, go to the "Jobs" tab and select "Create Job".

Choose type "Spark" and pick the corresponding files from your resource.

It is important that you stick to the following naming convention. Feel free to choose the remaining job settings as you wish e.g. scheduling options. 

1. LC_data_exploration:
    - Name: "LC_data_exploration"
    - Application File: "LC_data_exploration.py"
    
2. LC_KPI_reporting:
    - Name: "LC_KPI_reporting":
    - Application File: "LC_KPI_reporting.py"
    
3. LC_ml_scoring:
    - Name: "LC_ml_scoring"
    - Application File: "LC_ml_model.py"
    


#### Section 2 - Creating and scheduling an Airflow Job via CDE

NB: You cannot run Section 2 unless you have created the Jobs in Section 1. 

CDE uses Airflow for Job Orchestration. In order to create an Airflow job, go to the "Jobs" page and create one of type "Airflow".

Name the job as you'd like and choose the "LC_airflow_config.py" file. Execute or optionally schedule the job.

Once it has been created, open the job from the "Jobs" tab and navigate to the "Airflow UI" tab. 

Next, click on the "Code" icon. This is the Airflow DAG we contained in the "LC_airflow_config.py" file. 

NB: There are two types of operators: CDWOperator and CDEJobRunOperator. You can use both to trigger execution from the CDE and CDW services (with Spark and Hive respectively). More operators will be added soon including the ability to customize these. 



#### Section 3 - Creating and scheduling Spark Jobs via the CDE CLI from CML

We will download the CDE CLI into a CML project and schedule CDE jobs from there. 

NB: You can download the CDE CLI to your local machine and follow the same steps with [this tutorial](https://www.cloudera.com/tutorials/cdp-using-cli-api-to-automate-access-to-cloudera-data-engineering.html) by the Cloudera Marketing Team, which additionally contains an example of the CDE REST API.

###### Setup Steps

If you are working in CML on CDP Demo, the "00_bootstrap.py" script takes care of most of the setup steps for you. However, you will still need to manually execute a couple of steps, please follow this order:

1. Make sure LoanStats_2015_subset_071821.csv has been uploaded to Cloud Storage.
  * If you are using CDP Demo, the file is already located in s3a://demo-aws-2/data/LendingClub/LoanStats_2015_subset_071821.csv
  * If you are not using CDP Demo, the bootstrap file will automatically create a folder in Cloud Storage and load the file.
  * In case you are having issues, as of 7/18/21 a copy of the file has been included in the data folder.


2. Go to the CML Project Settings and add the following environment variables to the project:
  * WORKLOAD_USER: this is your CDP user
  * CDE_VC_ENDPOINT: navigate to the CDE VPC Cluster Details page and copy the "JOBS API URL", then save it as a CML environment variable.

![alt text](https://github.com/pdefusco/myimages_repo/blob/main/jobs_api_url.png)

3. Launch a CML Session with Workbench Editor.
  * Run the "00_bootstrap.py" file but only up until line 52 (highlight the lines of code you want to run and then click on "Run" -> "Run Lines" from the top bar)
  * Manually download the CDE CLI for Linux to your local machine from the CDE VPC Cluster Details page.
  
![alt_text](https://github.com/pdefusco/myimages_repo/blob/main/download_cde_cli.png)
  
  * Upload the executable in the CML project home page
  * Uncomment and execute lines 53 to 57 in "00_bootstrap.py"
  
###### Exercise Steps

From the same CML Session, open the "01_cde_cli_intro.py" file and execute the commands one by one. The script includes notes and an explanation of each command.



#### Documentation

For more information on the Cloudera Data Platform and its form factors please visit [this site](https://docs.cloudera.com/)
