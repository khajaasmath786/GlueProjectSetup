from pyspark.sql import SparkSession, Row
import pydeequ
from pydeequ.suggestions import *
from pydeequ.analyzers import *
from pydeequ.profiles import *
from pydeequ.checks import *
from pydeequ.verification import *
from copy import deepcopy
import os
import sys
import logging
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from datetime import datetime
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME'])

# Create Spark session with Glue JARs included
jars_path = os.path.join(os.getcwd(), "jars", "*")
sparkSession = SparkSession \
    .builder \
    .appName("MSSQL to CSV") \
    .config("spark.driver.extraClassPath", jars_path) \
    .config("spark.executor.extraClassPath", jars_path) \
    .getOrCreate()

sc = sparkSession.sparkContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")



## Creating Sample DataFrame
df = spark.sparkContext.parallelize([
            Row(emp_name="john1", emp_age=21, emp_sal=90,emp_dept = 'Science',emp_email='john1@gmail.com'),
            Row(emp_name="john2", emp_age=22, emp_sal=900,emp_dept = 'Social',emp_email='john2@gmail.com'),
            Row(emp_name="john3", emp_age=23, emp_sal=9000,emp_dept = 'Maths',emp_email='john3@gmail.com'),
            Row(emp_name="john4", emp_age=24, emp_sal=90000,emp_dept = 'Biology',emp_email='john4@gmail.com'),
            Row(emp_name="john5", emp_age=25, emp_sal=900000,emp_dept = 'Science',emp_email='john5@gmail.com'),
            Row(emp_name="john6", emp_age=23, emp_sal=10,emp_dept = 'Maths',emp_email='john6@gmail'),
            Row(emp_name="john7", emp_age=24, emp_sal=190,emp_dept = 'Biology',emp_email='john1@gmail.com'),
            Row(emp_name="john8", emp_age=25, emp_sal=9220,emp_dept = 'Science',emp_email='john1@gmail.com'),
            Row(emp_name="john9", emp_age=24, emp_sal=9680,emp_dept = 'Maths',emp_email='john7@gmail.com'),
            Row(emp_name="john10", emp_age=-25, emp_sal=958560,emp_dept = 'Science',emp_email='john1@gmail.com'),
            Row(emp_name="john11", emp_age=25, emp_sal='',emp_dept = 'Social',emp_email='john1@gmail.com'),
            Row(emp_name="john12", emp_age=26, emp_sal=9585680,emp_dept = 'Biology',emp_email='john1@gmail.com'),
            Row(emp_name="john13", emp_age=27, emp_sal=None,emp_dept = 'Science',emp_email='john9@gmail.com'),
            Row(emp_name="john14", emp_age=28, emp_sal=958580,emp_dept = 'Maths',emp_email='john1@gmail'),
            Row(emp_name="john15", emp_age=25, emp_sal=9585680,emp_dept = 'Biology',emp_email='john1@gmail.com'),
            Row(emp_name="john16", emp_age=26, emp_sal=950,emp_dept = 'Science',emp_email='john1gmail.com'),
            Row(emp_name="john17", emp_age=27, emp_sal=950,emp_dept = 'Social',emp_email='john1@gmail.com'),
            Row(emp_name="john18", emp_age=28, emp_sal=None,emp_dept = 'Biology',emp_email='john1@gmail.com')]).toDF()
df.show()
