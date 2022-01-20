from __future__ import print_function

import findspark
findspark.init()

import sys

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

if __name__ == "__main__":
    # if len(sys.argv) != 4:
    #     print("Usage  : spark_s3_integration.py <AWS_ACCESS_KEY_ID> <AWS_SECRET_ACCESS_KEY> <BUCKET_NAME>", file=sys.stderr)
    #     print("Example: spark_s3_integration.py ranga_aws_access_key ranga_aws_secret_key ranga-spark-s3-bkt", file=sys.stderr)
    #     exit(-1)
    #
    # awsAccessKey = sys.argv[1]
    # awsSecretKey = sys.argv[2]
    # bucketName = sys.argv[3]
    bucketName="aws-athena-query-results-965579072529-us-east-1"

    conf = (
        SparkConf()
            .setAppName("PySpark S3 Integration Example")
            .set("spark.hadoop.fs.s3a.access.key", "xxxx")
            .set("spark.hadoop.fs.s3a.secret.key", "xxxx")
            .set("spark.hadoop.fs.s3a.session.token","IQoJb3JpZ2luX2VjEN3//////////wEaCXVzLWVhc3QtMSJIMEYCIQDRmLwsaMAvNQNw5IKyVcZYi/m9HT0+O0GBDsFuUL9IDgIhAKkN/q4zUUcTyp+x06ZYWOfXJZS9rgiFeVdaG8sbFpQKKpsDCPb//////////wEQABoMOTY1NTc5MDcyNTI5IgzoNA1tFDRy84ixynwq7wJk5TsfBOMO7Og7E3XVEyqaSvLvVq4nhPWmCFAXfI7RB+ky64X58+brMakY9ha9qz8DdE93KDeZ66GDh9VtEZQmgbLixR7vPCZIKe1DaJAydTsy+YtILtW0FQrOsAmq+57PEOoEYJzzRq7du3N93XHRekTvAG02RpRQQ2fCxn6Ak8HQKluUYsn9xDcYItq0ejZegdp+s7KzP6K9Kug09R0adsf0NiS0vidQzLoen9jbJyib3VU5F7B4neGHgPR9IygtlSQwjF5GCF5PpDtfxBribILzGohKXhZtIpGjx9TTVbUlQde02gxHbapHCPzjUb+MbYnm0re5XRy36Fr5K1EKoq+18rusIH5shNGkTQ1PI3Cn46FWamsK7VIh2wDiN3hu/0ET2Gcsq2XUOD7aWQ3Sjo9CubU+84gWj5M80S3QFUr01clyPXWfrGfXUN2IKRAIZpWxij5IL3JPXqgtBP8T6sTRhXz0Gk4BtyU3czyWMP3ooY8GOqUBbdYQ1lasobAjhwv92rWKsKwEzfElSh9IWVLTRQE17bXGIPEdrQxWYeQbsyihjuADr356kzGYPQl8mWpUjLikn2W6THNrhnH8slAYlE2clHaTb7ulE1FbSQeGzPNjiXhD5FR+s0G6QuRnG6d3k1Ste/PnDJ8qTlRaEgJeGEjXTd4UkPFI6sgscbiwx9I7TeuhtarQOsOorqEpl9o+gRJhB8YgSn5g")
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("com.amazonaws.services.s3.enableV4", "true")
            .set("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
            .set("spark.hadoop.fs.s3a.endpoint", "")
            .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .set("spark.speculation", "false")
            .set("spark.jars", "aws-java-sdk-bundle-1.11.375.jar,hadoop-aws-3.2.2.jar")
            .set("spark.driver.extraClassPath","aws-java-sdk-bundle-1.11.375.jar,hadoop-aws-3.2.2.jar")
            .set("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
            .set("fs.s3a.experimental.input.fadvise", "random")
            .setIfMissing("spark.master", "local")
    )



    # Creating the SparkSession object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("SparkSession Created successfully")

    df = spark.read.json("s3a://clx-xxxxxx-qa/datasets/TDBR113.TWMS001/20212003-183540323.json")
    df.printSchema()
    df.show()


    Employee = Row("id", "name", "age", "salary")
    employee1 = Employee(1, "Ranga", 32, 245000.30)
    employee2 = Employee(2, "Nishanth", 2, 345000.10)
    employee3 = Employee(3, "Raja", 32, 245000.86)
    employee4 = Employee(4, "Mani", 14, 45000.00)

    employeeData = [employee1, employee2, employee3, employee4]
    employeeDF = spark.createDataFrame(employeeData)
    #employeeDF.printSchema()
    #employeeDF.show()

    # Define the s3 destination path
    s3_dest_path = "s3a://" + bucketName + "/asmath"
    print("s3 destination path "+s3_dest_path)

    # Write the data as Orc
    employeeOrcPath = s3_dest_path
    employeeDF.write.mode("overwrite").format("orc").save(employeeOrcPath)

    # Read the employee orc data
    employeeOrcData = spark.read.format("orc").load(employeeOrcPath);
    employeeOrcData.printSchema()
    employeeOrcData.show()

    # Write the data as Parquet
    employeeParquetPath = s3_dest_path + "/employee_parquet"
    employeeOrcData.write.mode("overwrite").format("parquet").save(employeeParquetPath)

    spark.stop()
    print("SparkSession stopped")