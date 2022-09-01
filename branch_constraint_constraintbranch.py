#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import (current_date, date_sub, dayofmonth, month,
                                   to_date, year)

from datetime import date, timedelta, datetime
import re
import os.path


# In[2]:


def daterange(start_date, end_date):
    """Create a date range generator

    Args:
        start_date (str): start date
        end_date (str): end date

    Returns:
        a generator for date range

    """
    for n in range(int((end_date - start_date).days + 1)):
        yield start_date + timedelta(n)


# In[3]:


def create_spark_date_filter(start_date, end_date):
    """ Constructs a Spark Dataframe.filter() that will match the columns 'year','month','day' to the date range between the start_date (inclusive) and end_date arguments (inclusive)

    Args:
        start_date (str):  "2021-6-1"
        end_date (str): "2021-6-2"

    Returns:
        filter (str)

    Example:
        start_date =  date(2021, 6, 1)
        end_date =  date(2021, 6, 2)
        returns '(year = 2021 and month = 06 and day = 01) or (year = 2021 and month = 06 and day = 02)'
    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    filter = ""
    # create a list of strings '(year == %Y and month == %m and day == %d)'
    times = [single_date.strftime('(year == %Y and month == %m and day == %d)') for single_date in daterange(start_date, end_date)]
    # join list
    filter = " or ".join(times)
    return filter


# In[4]:


def interpret_input_date_arg(date_arg):
    """ Checks the date argument for special cases:
     - 'TODAY' is replaced with the current date
     - 'n_DAYS_AGO' is replaced the date n days ago

     Args:
         date_arg (str): ideally "TODAY" or "n_DAYS_AGO"

     Return:
         a date (str if no match, or date if it matches today's date or a relative date to today)
    """
    # check if it's supposed to be today's date
    if "TODAY".casefold() == str(date_arg).casefold():
        return date.today().strftime("%Y-%m-%d")

    # check for a relative date to today
    n_days_ago = r'(\d+)_DAYS_AGO'
    match = re.match(n_days_ago, str(date_arg))
    if match:
        days = match.groups(1)[0]
        return datetime.strftime(date.today() - timedelta(int(days)), "%Y-%m-%d")

    # no special matches, return the original date string
    return date_arg


# In[5]:


def get_version_number():
    if os.path.exists('version.yaml'):
        version = open('version.yaml', 'r').read()
        if (len(version.strip()) != 0):
            return version
    return 'No version defined'


# In[6]:


# set top level variables
database_raw = "raw"
database_optimise = "optimise"
database_mart = "mart"
table_branch = "branch_0"
table_constraint = "constraint_0"
table_constraintbranch = "constraintbranch_0"


# In[7]:


# initiate spark session
spark = SparkSession \
    .builder \
    .appName("full_branch_constraint_constraintbranch") \
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region", "us-east-2")\
    .config("spark.yarn.access.hadoopFileSystems", "s3a://go01-demo/")\
    .enableHiveSupport() \
    .getOrCreate()


# In[8]:


# Setup logger
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)
logger.info("Job is running on "+ get_version_number())


# In[9]:


env = "prod"
bucket = "s3a://go01-demo/tp-cdp-datalake-landing-%s" % (env)
project = "datalake"


# In[10]:


# hive configuration
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.hive.mapred.supports.subdirectories", "true")
spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")


# In[11]:


# drop temp view
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_branch))


# In[12]:


# create raw database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))


# In[13]:


# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_branch))


# In[14]:


# read last 3 day's of partitions from landing parquet files
basePath = "s3a://go01-demo/tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_branch)
inputPath = "s3a://go01-demo/tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_branch)



# temporary: setup test environment but sharing the same kinesis source as dev
#share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
#if share_dev_source and share_dev_source == "true":
#    basePath = "s3a://go01-demo/tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
#        "prod", database_raw, table_branch)
#    inputPath = "s3a://go01-demo/tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
#        "prod", database_raw, table_branch)

inputDf_raw_branch = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)


# In[ ]:





# In[28]:


inputDf_raw_branch.head()


# In[18]:


firstDay = '2021-08-01'
lastDay = '2021-08-03'


# In[19]:


# date range for job
#firstDay=interpret_input_date_arg(spark.conf.get("spark.driver.firstDay", '2_DAYS_AGO'))      # firstDay="2021-11-05"
#lastDay=interpret_input_date_arg(spark.conf.get("spark.driver.lastDay", 'TODAY'))             # lastDay="2021-11-07"
logger.info(f'Data date range ("firstDay" -> "lastDay"): "{firstDay}" -> "{lastDay}"')


# In[20]:


print("First day")
print(firstDay)
print("Last day")
print(lastDay)


# In[21]:


inputDf_raw_branch \
    .filter(create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_mkt_%s" % (table_branch))


# In[31]:


# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    branchid string,
    branchname string,
    branchtype string,
    currentnearopen int,
    currentneardead int,
    currentfaropen int,
    currentfardead int,
    currentnearmw float,
    currentfarmw float,
    currentnearflow float,
    currentfarflow float,
    currentlimit float,
    threewindingtype int,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_branch, bucket, env, project, database_raw, table_branch))


# In[32]:


database_raw + "." + table_branch


# In[33]:


# insert into hive table
spark.sql("""
insert
into %s.%s
partition(modified)
from raw_mkt_%s
select
    operation,
    timestamp,
    primarykey,
    branchid,
    branchname,
    branchtype,
    currentnearopen,
    currentneardead,
    currentfaropen,
    currentfardead,
    currentnearmw,
    currentfarmw,
    currentnearflow,
    currentfarflow,
    currentlimit,
    threewindingtype,
    createddate,
    modifieddate,
    to_date(from_unixtime(cast(round(modifieddate/1000) as bigint))) as modified
""" % (database_raw, table_branch, table_branch))


# optimise_branch_0

# In[34]:


# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_optimise, bucket, env, project, database_optimise))


# In[35]:


spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    branchid string,
    branchname string,
    branchtype string,
    currentnearopen int,
    currentneardead int,
    currentfaropen int,
    currentfardead int,
    currentnearmw float,
    currentfarmw float,
    currentnearflow float,
    currentfarflow float,
    currentlimit float,
    threewindingtype int,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_optimise, table_branch, bucket, env, project, database_optimise, table_branch))


# In[37]:


insertDf_raw_branch = spark.sql("""
select
    operation,
    timestamp,
    primarykey,
    branchid,
    branchname,
    branchtype,
    currentnearopen,
    currentneardead,
    currentfaropen,
    currentfardead,
    currentnearmw,
    currentfarmw,
    currentnearflow,
    currentfarflow,
    currentlimit,
    threewindingtype,
    createddate,
    modifieddate,
    modified
from %s.%s
where
    (modified>=date_sub(to_date("%s", "yyyy-MM-dd"), 1))
""" % (database_raw, table_branch, firstDay))


# In[38]:


insertDf_raw_branch.write.insertInto("%s.%s" % (database_optimise, table_branch), overwrite=True)


# raw_constraintbranch_0

# In[39]:


# create temp table
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_constraint))

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_constraint))


# In[40]:


# read last 3 day's of partitions from landing parquet files
basePath = "s3a://go01-demo/tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_constraintbranch)
inputPath = "s3a://go01-demo/tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_constraintbranch)


# In[41]:


basePath


# In[42]:


inputDf_raw_constraintbranch = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)


# In[ ]:




