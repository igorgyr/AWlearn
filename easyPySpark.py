import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[1]").appName("AWSpark").getOrCreate()
df = spark.read.option("header","true").csv("s3a://aws-stocks-dataset/B*").limit(1000)
df.show()
df = df.withColumn("High", col("High").cast("float"))
df = df.withColumn("Low", col("Low").cast("float"))
df = df.withColumn("diff_High_Low", col("High") - col("Low"))
df = df.withColumn("High_int", col("High").cast("int"))
df = df.withColumn("date_dt", to_date(col("Date"),"dd-MM-yyyy"))
df.show()
df.printSchema()
df.groupBy("Date").max("High").show()
ddf = DynamicFrame.fromDF(df, glueContext, "ddf")

glueContext.write_dynamic_frame.from_options(ddf, 
connection_type="s3", 
connection_options={"path": "s3://aws-stocks-dataset-output/igorNotJetGreat", "partitionKeys": ["Date"]}, format="parquet")
# local

df.write.parquet("/igor/pySparkRes/resDay4.parquet")
