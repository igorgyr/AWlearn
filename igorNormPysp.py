import json
from flaten import flatten
import hashlib.md5 
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[1]").appName("AWSpark").getOrCreate()
df = spark.read.option("header","true").csv("s3a://aws-stocks-dataset/C*")
@udf(returnType=StringType())
def hash(str):
    return hashlib.md5(str.encode("UTF-8")).hexdigest()

hashUDF = udf(lambda v: hash(v),StringType())
df.withColumn("hash_high", hashUDF(col("High"))).show()
df = df.withColumn("High_int", col("High").cast("int"))
df = df.withColumn("Date", to_date(col("Date"),"dd-MM-yyyy"))
dfJs = spark.read.jsom("s3a://spark-concerts-json/quarter-json")
dfJsF =  flatten(dfJs)
dfJsF.show()
dfJsF.printSchema()
dfJsF = dfJsF.withColumn("Date", to_date(col("Date"),"dd-MM-yyyy"))
df = df.limit(100)
dfJsF = dfJsF.limit(100)
newDF = df.join(dfJsF,df.Data ==  dfJsF.Data,"inner")
print(newDF.count())
