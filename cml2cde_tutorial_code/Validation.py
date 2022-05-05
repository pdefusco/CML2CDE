from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import sys

spark = SparkSession \
    .builder \
    .appName("Pyspark PPP ETL") \
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region", "us-east-2")\
    .config("spark.yarn.access.hadoopFileSystems", "s3a://demo-aws-go02")\
    .getOrCreate()  
    
print(f"Retrieve 1000 records for validation \n")
df = spark.sql("Select * from TexasPPP.loan_data limit 1000")

df.\
  write.\
  mode("append").\
  saveAsTable("ValidationTable", format="parquet")