#
#Copyright (c) 2022 Cloudera, Inc. All rights reserved.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import sys

spark = SparkSession \
    .builder \
    .appName("Pyspark PPP ETL") \
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region", "us-east-2")\
    .config("spark.yarn.access.hadoopFileSystems", "s3a://demo-aws-go02")\
    .getOrCreate()  
    
#Path of our file in S3
input_path ='s3a://demo-aws-go02/datalake/cde-demo/PPP-Sub-150k-TX.csv'

#This is to deal with tables existing before running this code. Not needed if you're starting fresh.
#spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

#Bring data into Spark from S3 Bucket
base_df=spark.read.option("header","true").option("inferSchema","true").csv(input_path)
#Print schema so we can see what we're working with
print(f"printing schema")
base_df.printSchema()

#Filter out only the columns we actually care about
filtered_df = base_df.select("LoanAmount", "City", "State", "Zip", "BusinessType", "NonProfit", "JobsRetained", "DateApproved", "Lender")

#This is a Texas only dataset but lets do a quick count to feel good about it
print(f"How many TX records did we get?")
tx_cnt = filtered_df.count()
print(f"We got: %i " % tx_cnt)

#Create the database if it doesnt exist
print(f"Creating TexasPPP Database \n")
spark.sql("CREATE DATABASE IF NOT EXISTS TexasPPP")
spark.sql("SHOW databases").show()

print(f"Inserting Data into TexasPPP.loan_data table \n")

#insert the data
filtered_df.\
  write.\
  mode("append").\
  saveAsTable("TexasPPP"+'.'+"loan_data", format="parquet")

#Another sanity check to make sure we inserted the right amount of data
print(f"Number of records \n")
spark.sql("Select count(*) as RecordCount from TexasPPP.loan_data").show()

print(f"Retrieve 15 records for validation \n")
spark.sql("Select * from TexasPPP.loan_data limit 15").show()







