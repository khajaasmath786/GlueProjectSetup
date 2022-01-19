from __future__ import print_function
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
    bucketName="clx-datawarehouse-qa"

    conf = (
        SparkConf()
            .setAppName("PySpark S3 Integration Example")
            .set("spark.hadoop.fs.s3a.access.key", "ASIA6BUH7MQI25BK3OOD")
            .set("spark.hadoop.fs.s3a.secret.key", "iYRcmHZ+rHY1oW/i8J8vYPoPt0vDSPEXQLozhqiO")
            .set("spark.hadoop.fs.s3a.session.token","IQoJb3JpZ2luX2VjEMH//////////wEaCXVzLWVhc3QtMSJHMEUCICuSdL0a/0HOAdeBLhWA1yIsyyXibr2kDWZQm4XOePKCAiEA8LNnAMPdsgt+HSh7el0v3Y7wk4gCRPuFLUCxi9oRhlsqmwMI2v//////////ARAAGgw5NjU1NzkwNzI1MjkiDHA4uK1gHd36lrJtaCrvAtyd4cxs66hcj8yPDrkLIAbwzn4Zf3v+0QdPqdGC1PGCky+GjYo+EwpFCos7XSflNqxFLW4bdN0uq2Dy7cKTzyAGR6GBGsbpHHcL7tTEdYx8SvE+qDUQDkd9tpLVHozSAPWwgJwVMzwzBqoVgQtLLTTQn3kVp9H+CaH2PvpPZQJQuERUGT7DzPclav0cw0a10Sw7FmYHjU1iRXuwqLmP3OjtwWk019mznwn/MFjliWoxSMwfcazDif4dsJC3nHizC5pdCs2m2ulkO1jbuNv4U1K4MThsV+Rwm7QAEyRzkOS/9BICXxe72PTqxn75gASbFp+MdKrjG1/9sBzPIdoIYHa7WT+XpgTrhmZwR5d/4OlIG8pTLlyZDMJ8NRdxFLGMnMX29DaxKGHAm7urVXRKQWU8Zzd8Nz5cSBYmuj1wWvRNc/KvBJbqAdEDVvi3IskPiBqvv3UpO0eAZu6JhZu42oc8C03jX3CaEzoCoO0afvow69ebjwY6pgFMU01ZL20BDzB2OZ+sMMINFsKe4Oqnp2YvfQhE1mFKjofLj9JLbk12jHqcoNcXWII+xFUxXthx8EJoCRshKW3YdDgd9uqp4C3tDMywAXuKZ91q7eVmojS659pd6hsr5pKFTmDjKpU4v9zFqxu4AktoyeWr4mnhdpZA10UyV0ijWgJl6jf2nZm9rNOygnlxUVN+KmWeGL38AFpQQtER30vd933lms1H")
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("com.amazonaws.services.s3.enableV4", "true")
            .set("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
            .set("spark.hadoop.fs.s3a.endpoint", "")
            .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .set("spark.speculation", "false")
            .set("spark.jars", "aws-java-sdk-1.7.4.jar,hadoop-aws-2.7.3.jar")
            .set("spark.driver.extraClassPath","aws-java-sdk-1.7.4.jar,hadoop-aws-2.7.3.jar")
            .set("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
            .set("fs.s3a.experimental.input.fadvise", "random")
            .setIfMissing("spark.master", "local")
    )

    # Creating the SparkSession object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("SparkSession Created successfully")

    Employee = Row("id", "name", "age", "salary")
    employee1 = Employee(1, "Ranga", 32, 245000.30)
    employee2 = Employee(2, "Nishanth", 2, 345000.10)
    employee3 = Employee(3, "Raja", 32, 245000.86)
    employee4 = Employee(4, "Mani", 14, 45000.00)

    employeeData = [employee1, employee2, employee3, employee4]
    employeeDF = spark.createDataFrame(employeeData)
    employeeDF.printSchema()
    employeeDF.show()

    # Define the s3 destination path
    s3_dest_path = "s3a://" + bucketName + "/datasets"
    print("s3 destination path "+s3_dest_path)

    # Write the data as Orc
    employeeOrcPath = s3_dest_path + "/employee_orc"
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