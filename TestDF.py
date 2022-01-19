import requests
import sys
import os
import logging
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from datetime import date
from pyspark.context import SparkContext
import sys
import calendar
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import lpad
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
import json
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME'])

 #Create Spark session with Glue JARs included
jars_path = os.path.join(os.getcwd(), "jars", "*")
spark = SparkSession \
    .builder \
    .appName("MSSQL to CSV") \
    .config("spark.driver.extraClassPath", jars_path) \
    .config("spark.executor.extraClassPath", jars_path) \
    .getOrCreate()
sc = spark.sparkContext
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "ASIA6BUH7MQI25BK3OOD")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "iYRcmHZ+rHY1oW/i8J8vYPoPt0vDSPEXQLozhqiO")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.session.token" , "IQoJb3JpZ2luX2VjEMH//////////wEaCXVzLWVhc3QtMSJHMEUCICuSdL0a/0HOAdeBLhWA1yIsyyXibr2kDWZQm4XOePKCAiEA8LNnAMPdsgt+HSh7el0v3Y7wk4gCRPuFLUCxi9oRhlsqmwMI2v//////////ARAAGgw5NjU1NzkwNzI1MjkiDHA4uK1gHd36lrJtaCrvAtyd4cxs66hcj8yPDrkLIAbwzn4Zf3v+0QdPqdGC1PGCky+GjYo+EwpFCos7XSflNqxFLW4bdN0uq2Dy7cKTzyAGR6GBGsbpHHcL7tTEdYx8SvE+qDUQDkd9tpLVHozSAPWwgJwVMzwzBqoVgQtLLTTQn3kVp9H+CaH2PvpPZQJQuERUGT7DzPclav0cw0a10Sw7FmYHjU1iRXuwqLmP3OjtwWk019mznwn/MFjliWoxSMwfcazDif4dsJC3nHizC5pdCs2m2ulkO1jbuNv4U1K4MThsV+Rwm7QAEyRzkOS/9BICXxe72PTqxn75gASbFp+MdKrjG1/9sBzPIdoIYHa7WT+XpgTrhmZwR5d/4OlIG8pTLlyZDMJ8NRdxFLGMnMX29DaxKGHAm7urVXRKQWU8Zzd8Nz5cSBYmuj1wWvRNc/KvBJbqAdEDVvi3IskPiBqvv3UpO0eAZu6JhZu42oc8C03jX3CaEzoCoO0afvow69ebjwY6pgFMU01ZL20BDzB2OZ+sMMINFsKe4Oqnp2YvfQhE1mFKjofLj9JLbk12jHqcoNcXWII+xFUxXthx8EJoCRshKW3YdDgd9uqp4C3tDMywAXuKZ91q7eVmojS659pd6hsr5pKFTmDjKpU4v9zFqxu4AktoyeWr4mnhdpZA10UyV0ijWgJl6jf2nZm9rNOygnlxUVN+KmWeGL38AFpQQtER30vd933lms1H")

glueContext = GlueContext(sc)


# sc = SparkContext()
# glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

df = spark.read.json("s3://clx-datawarehouse-qa/datasets/TDBR113.TWMS001/20212003-183540323.json")
df.printSchema()
df.show()

SystemExit.code(0)

from pyspark.sql import SparkSession, Row
df_rows = [
        Row(emp_name="mohan1", emp_age=21, emp_sal=90, emp_dept='Science', emp_email='mohan1@gmail.com'),
        Row(emp_name="mohan2", emp_age=22, emp_sal=900, emp_dept='Scocial', emp_email='mohan2@gmail.com'),
        Row(emp_name="mohan3", emp_age=23, emp_sal=9000, emp_dept='Maths', emp_email='mohan3@gmail.com'),
        Row(emp_name="mohan4", emp_age=24, emp_sal=90000, emp_dept='Biology', emp_email='mohan4@gmail.com'),
        Row(emp_name="mohan5", emp_age=25, emp_sal=900000, emp_dept='Science', emp_email='mohan5@gmail.com'),
        Row(emp_name="mohan6", emp_age=23, emp_sal=10, emp_dept='Maths', emp_email='mohan6@gmail'),
        Row(emp_name="mohan7", emp_age=24, emp_sal=190, emp_dept='Biology', emp_email='mohan1@gmail.com'),
        Row(emp_name="mohan8", emp_age=25, emp_sal=9220, emp_dept='Science', emp_email='mohan1@gmail.com'),
        Row(emp_name="mohan9", emp_age=24, emp_sal=9680, emp_dept='Maths', emp_email='mohan7@gmail.com'),
        Row(emp_name="mohan10", emp_age=-25, emp_sal=958560, emp_dept='Science', emp_email='mohan1@gmail.com'),
        Row(emp_name="mohan11", emp_age=25, emp_sal='', emp_dept='Scocial', emp_email='mohan1@gmail.com'),
        Row(emp_name="mohan12", emp_age=26, emp_sal=9585680, emp_dept='Biology', emp_email='mohan1@gmail.com'),
        Row(emp_name="mohan13", emp_age=27, emp_sal=None, emp_dept='Science', emp_email='mohan9@gmail.com'),
        Row(emp_name="mohan14", emp_age=28, emp_sal=958580, emp_dept='Maths', emp_email='mohan1@gmail'),
        Row(emp_name="mohan15", emp_age=25, emp_sal=9585680, emp_dept='Biology', emp_email='mohan1@gmail.com'),
        Row(emp_name="mohan16", emp_age=26, emp_sal=950, emp_dept='Science', emp_email='mohan1gmail.com'),
        Row(emp_name="mohan17", emp_age=27, emp_sal=950, emp_dept='Scocial', emp_email='mohan1@gmail.com'),
        Row(emp_name="mohan18", emp_age=28, emp_sal=None, emp_dept='Biology', emp_email='mohan1@gmail.com')]
## Creating Sample DataFrame
df = spark.sparkContext.parallelize(df_rows).toDF()

df.show()


job.commit()
