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

None = DAG(
    'airflow-pipeline-demo',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False
    )

cdw_query = """Create external table external_table(name string,age int,address string,zip int)row format delimitedfields terminated by ','stored as textfilelocation '/test/abc';"""

Create_External_Table_Step = CDWOperator(
    task_id="Create_External_Table",
    dag=None,
    cli_conn_id="hive_conn",
    hql=cdw_query,
    schema='default',
    ### CDW related args ###
    use_proxy_user=False,
    query_isolation=True
)

cdw_query = """Create Table orc_table(name string, -- Concate value of first name and last name with space as seperatoryearofbirth int,age int, -- Current year minus year of birthaddress string,zip int)STORED AS ORC;"""

Create_orc_Table_Step = CDWOperator(
    task_id="Create_orc_Table",
    dag=None,
    cli_conn_id="hive_conn",
    hql=cdw_query,
    schema='default',
    ### CDW related args ###
    use_proxy_user=False,
    query_isolation=True
)

cdw_query = """use "default"; -- input from Oozieinsert into table orc_tableselectconcat(first_name,' ',last_name) as name,yearofbirth,year(from_unixtime) --yearofbirth as age,address,zipfrom external_table;"""

Insert_into_Table_Step = CDWOperator(
    task_id="Insert_into_Table",
    dag=None,
    cli_conn_id="hive_conn",
    hql=cdw_query,
    schema='default',
    ### CDW related args ###
    use_proxy_user=False,
    query_isolation=True
)

