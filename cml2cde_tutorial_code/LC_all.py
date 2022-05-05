from pyspark.sql import SparkSession
from pyspark.sql import functions as F

## Launching Spark Session

spark = SparkSession \
    .builder \
    .appName("Lending Club") \
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region", "us-east-2")\
    .config("spark.yarn.access.hadoopFileSystems", "s3a://demo-aws-go02")\
    .getOrCreate()  

## Creating Spark Dataframe from raw CSV datagov

df = spark.read.option('inferschema','true').csv(
  "s3a://demo-aws-go02/data/LendingClub/LoanStats_2015_subset_071821.csv",
  header=True,
  sep=',',
  nullValue='NA'
)

## Printing number of rows and columns:
print('Dataframe Shape')
print((df.count(), len(df.columns)))

## Showing Different Loan Status Values
df.select("loan_status").distinct().show()

## Types of Loan Status Aggregated by Count

print(df.groupBy('loan_status').count().show())

#by using like function
df.groupBy("grade","loan_status").\
count().\
filter(F.lower(F.col("loan_status")).like("late%")).\
groupby('grade').\
sum().\
sort(F.asc('grade')).\
show()

df_late = df.groupBy("grade","loan_status").\
count().\
filter(F.lower(F.col("loan_status")).like("late%")).\
groupby('grade').\
sum().\
sort(F.asc('grade'))

#by using like function
df_delinq = df.groupBy("grade").\
max("inq_last_6mths").\
na.drop().\
sort(F.asc('grade'))

#This time we need to cast the attribute we are working with to numeric before we can create a similar dataframe:
df = df.withColumn('loan_amnt', F.col('loan_amnt').cast('int'))

#by using like function
df_ann_inc = df.groupBy("grade").\
mean("loan_amnt").\
na.drop().\
sort(F.asc('grade'))

df_delinq.alias('a').join(df_ann_inc.alias('b'),F.col('b.grade') == F.col('a.grade')).\
join(df_late.alias('c'), F.col('b.grade') == F.col('c.grade')).\
select(F.col('a.grade'), F.col('a.max(inq_last_6mths)'), F.col('b.avg(loan_amnt)'), F.col('c.sum(count)').alias('default_count')).\
show()

## Spark sql

spark.sql("show databases").show()

spark.sql("show tables").show()

df.write.format('parquet').mode("overwrite").saveAsTable('default.LC_tab_cde_demo')

#Running SQL like queries on the dataframe 
group_by_grade = spark.sql("SELECT grade, MEAN(loan_amnt) FROM LC_tab_cde_demo WHERE grade IS NOT NULL GROUP BY grade ORDER BY grade")

group_by_grade.show()

group_by_subgrade = spark.sql("SELECT sub_grade, MEAN(loan_amnt), MEAN(annual_inc), SUM(is_default) FROM LC_tab_cde_demo GROUP BY sub_grade ORDER BY sub_grade")

df = spark.sql("SELECT * FROM default.LC_tab_cde_demo")

#Creating list of categorical and numeric features
num_cols = [item[0] for item in df.dtypes if item[1].startswith('in') or item[1].startswith('dou')]

df = df.select(*num_cols)

df = df.dropna()

df = df.select(['acc_now_delinq', 'acc_open_past_24mths', 'annual_inc', 'avg_cur_bal', 'is_default'])

## Printing number of rows and columns:
print('Dataframe Shape')
print((df.count(), len(df.columns)))

#Creates a Pipeline Object including One Hot Encoding of Categorical Features
def make_pipeline(spark_df):

    for c in spark_df.columns:
        spark_df = spark_df.withColumn(c, spark_df[c].cast("float"))

    stages= []

    cols = ['acc_now_delinq', 'acc_open_past_24mths', 'annual_inc', 'avg_cur_bal']

    #Assembling mixed data type transformations:
    assembler = VectorAssembler(inputCols=cols, outputCol="features", handleInvalid='skip')
    stages += [assembler]

    #Scaling features
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    stages += [scaler]

    #Logistic Regression
    lr = LogisticRegression(featuresCol='scaledFeatures', labelCol='is_default', maxIter=10, regParam=0.3, elasticNetParam=0.4)
    stages += [lr]

    #Creating and running the pipeline:
    pipeline = Pipeline(stages=stages)
    pipelineModel = pipeline.fit(spark_df)
    out_df = pipelineModel.transform(spark_df)

    return out_df, pipelineModel

df_model, pipelineModel = make_pipeline(df)

input_data = df_model.rdd.map(lambda x: (x["is_default"], float(x['probability'][1])))

## Outputting predictions from Logistic Regression
predictions = spark.createDataFrame(input_data, ["is_default", "probability"])