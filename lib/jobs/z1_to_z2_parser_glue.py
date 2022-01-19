"""Module to parse  Connect Logs Data.
This script reads JSON records from s3 bucket and converts into a parquet data is a simple 1-1 JSON"""
# cl_z1_to_z2_parser

import sys
import boto3
import logging
import json
from awsglue.job import Job
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
import ast
from pyspark.sql.types import StructType, StringType, StructField
import pyspark.sql.functions as F
from z1_to_z2_json_parser import JSONParser
import os
from pyspark.sql import SparkSession

# read input parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'hook_lambda_function','schema_path','output_path','object_id_list'])

 #Create Spark session with Glue JARs included
# jars_path = os.path.join(os.getcwd(), "jars", "*")
# spark = SparkSession \
#     .builder \
#     .appName("MSSQL to CSV") \
#     .config("spark.driver.extraClassPath", jars_path) \
#     .config("spark.executor.extraClassPath", jars_path) \
#     .getOrCreate()
# sc = spark.sparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.caseSensitive", "true")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# get logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

# create class object
obj = JSONParser(logger)

def __get_file_name_and_received_date(file_path: str):
    """get file name & received date from file path"""
    file_name = file_path.split("/")[-1]
    file_path_split = file_path.split("/")
    if '.' in file_path_split[-2] and '_ct' in file_path_split[-2]:
        if '-' in file_path_split[-1]:
            received_date = file_path_split[-1].split('-')[0]
        else:
            received_date = file_path_split[-1].split('_')[0]
    else:
        received_date = ''
    # received_date = "".join(file_path.split("/")[-5:-1])
    return received_date


def __get_obj_key_and_obj_value(file_path: str):
    """
    Parameters:
        file_path(str):s3 files path
        ex:"s3://wms-z1-test-bucket/data/testdrive.TeamsHalf__ct/
        20211112T000000_20211112T010000/20211112-001522529.json.gz"
    Returns:
        obj_key_1(str): table name ex: testdrive.TeamsHalf__ct
        file_path(str): object key in the table name folder ex: same as input file_path
        partition_key(str): Partition key in case the folder is incremental data
        ex:20211112
    """
    file_path_split = file_path.split("/")
    if '.' in file_path_split[-3] and '_ct' in file_path_split[-3]:
        obj_key_1 = file_path_split[-3]
        partition_key = file_path_split[-2].split('_')[0]
    elif '.' in file_path_split[-2]:
        obj_key_1 = file_path_split[-2]
        partition_key = ''
    else:
        obj_key_1 = ''
        partition_key = ''
    return obj_key_1, file_path, partition_key


if __name__ == "__main__":

    output_bucket = args['output_path']
    object_id_list = args['object_id_list']
    hook_lambda_function = args['hook_lambda_function']
    #database_name = args['database_name']
    object_id_list = ["s3://clx-datawarehouse-qa/datasets/TDBR113.TWMS001/20212003-183540323.json"]
    #object_id_list = ast.literal_eval(object_id_list)
    schema_path = args['schema_path']
    schema_bucket = schema_path.replace("s3://", "").split('/', 1)[0]
    schema_prefix = schema_path.replace("s3://", "").split('/', 1)[1]
    input_path_list = []
    schema_not_found_paths = []
    count_mismatch_paths = []
    exception_error_paths = []
    payload_msg = {"detail": {"jobName": args['JOB_NAME'],
                              "failure_message": f"glue job failed with List of files : {object_id_list}"}}
    try:
        obj_dict = {}
        for i in object_id_list:
            obj_key, obj_value, partition_key = __get_obj_key_and_obj_value(i)
            if obj_key != '':
                if obj_key in obj_dict:
                    temp_list = obj_dict[obj_key]
                    del obj_dict[obj_key]
                    obj_dict[obj_key] = temp_list + [obj_value]
                else:
                    obj_dict[obj_key] = [obj_value]

        common_schema_path = f"{schema_path}"
        # get schema files
        schema_list = obj.get_all_s3_objects(schema_bucket, schema_prefix)
        schema_map = {}
        logger.info("Create Schema to file mapping")
        logger.info(schema_list)
        # map schema to input event file
        for event, value in obj_dict.items():
            event_type = event.split('.')[-1].replace('__ct','')
            common_schema_event_path = common_schema_path + event_type
            logger.info(common_schema_event_path)
            if common_schema_event_path + ".json" in schema_list:
                schema_map[event.replace('.', '_')] = common_schema_event_path + ".json", event.replace('.', '_')
            elif common_schema_path + event_type.rsplit('.', 1)[0] + ".json" in schema_list:
                schema_map[event] = \
                    common_schema_event_path.rsplit('.', 1)[0] + ".json", \
                    (event_type.rsplit('.', 1)[0]).split('.', 1)[1].replace('.', '_')
            else:
                logger.error(f"schema not found in given list of schemas")
                schema_not_found_paths.append(event)

        logger.info("Schema to file mapping is completed")
        logger.info(schema_map)
        for key, value in obj_dict.items():
            try:
                key = key.replace('.', '_')
                if '__ct' in key:
                    input_path_list = value
                    if len(input_path_list) > 0:
                        # UDF & UDF Schema to return filename and received date
                        udf_schema = StructType([
                            StructField("input_file_name", StringType(), False),
                            StructField("received_date", StringType(), False)])
                        get_file_name_date_udf = F.udf(lambda z: __get_file_name_and_received_date(z))
                        logger.info("Start creating a dataframe")
                        logger.info(key)
                        if key in schema_map:
                            schema_and_root = schema_map[key]
                            df, source_count_df = obj.parse_json_files(spark, schema_and_root[0], input_path_list[0])
                            source_df = df
                            source_df.printSchema()
                        else:
                            logger.info(f"Schema not for table {key} so proceeding without schema")
                            df = glueContext.create_dynamic_frame_from_options("s3", {'paths': input_path_list},
                                                                               format="json")
                            source_df = df.toDF()

                        # Null Type Check
                        my_schema = list(source_df.schema)
                        null_cols = []

                        # iterate over schema list to filter for NullType columns
                        for st in my_schema:
                            if str(st.dataType) == 'NullType':
                                null_cols.append(st)

                        # cast null type columns to string (or whatever you'd like)
                        for ncol in null_cols:
                            mycolname = str(ncol.name)
                            source_df = source_df.withColumn(mycolname, source_df[mycolname].cast('string'))

                        # Get input filename & received date
                        parsed_df = source_df.withColumn("input_file_path", F.input_file_name())
                        parsed_df = parsed_df.withColumn("received_date",
                                                         get_file_name_date_udf(F.col('input_file_path')))
                        # parsed_df = parsed_df.withColumn("received_date",
                        #                                  F.split(F.col('input_file_path'), '/').getItem(3))
                        # parsed_df = parsed_df.withColumn("received_date",
                        #                                  F.split(F.col('received_date'), '_').getItem(0))
                        parsed_df = parsed_df.withColumn("received_date", F.regexp_replace('received_date', 'T', ''))
                        # split date to year, month, day and hour to create hourly partitions
                        parsed_df = parsed_df.withColumn("received_date",
                                                         F.to_timestamp(F.col("received_date"), 'yyyyddMM')). \
                            withColumn("year", F.year(F.col("received_date"))). \
                            withColumn("month", F.month(F.col("received_date"))). \
                            withColumn("day", F.dayofmonth(F.col("received_date"))). \
                            withColumn("hour", F.hour(F.col("received_date")))
                        parsed_df.printSchema()
                        bytes_payload_msg = json.dumps(payload_msg).encode('utf-8')
                        lambda_client = boto3.client('lambda', region_name='us-east-1')
                        # this is a simple read and process but below is in place for compliance
                        if(source_df.count() != parsed_df.count()):
                            logger.info("Source data frame and Target dataframe counts dont match:")
                            logger.info(f"Target data frame count {source_df.count()}")
                            logger.info(f"Target data frame count {parsed_df.count()}")
                            response = lambda_client.invoke(FunctionName=hook_lambda_function, Payload=bytes_payload_msg)
                            logger.info(f"lambda trigger response : {response}")
                            raise Exception("Count mismatch")

                        # Data validation to check if the data in columns are not offset or its contains a corrupt record
                        if((parsed_df.columns.
                            __contains__("input_file_name") and parsed_df.
                            filter(~F.col("input_file_name").
                                   rlike(".gz")).count() > 0) or (parsed_df.columns.__contains__("_corrupt_record"))):
                            logger.info(f"Trigger lambda to Raise a Hook Alert with following error message: {payload_msg}")
                            response = lambda_client.invoke(FunctionName=hook_lambda_function, Payload=bytes_payload_msg)
                            logger.info(f"lambda trigger response : {response}")
                            raise Exception("data offset or corrput record")

                        # write the data frame to the output path
                        logger.info(output_bucket)
                        output_path = output_bucket + "/" + key
                        logger.info(output_path)
                        parsed_df = parsed_df.drop('received_date','input_file_path')
                        parsed_df.show()
                        parsed_df.write.mode("append").partitionBy("year", "month", "day", "hour").parquet(output_path)
                        # parsed_ddf = DynamicFrame.fromDF(parsed_df, glueContext, "parsed_df")
                        # glueContext.write_dynamic_frame.from_options(frame=parsed_ddf, connection_type="s3",
                        #                                              connection_options={"path": output_path,
                        #                                                                  "partitionKeys": ["year",
                        #                                                                                   "month",
                        #                                                                                   "day",
                        #                                                                                   "hour"]},
                        #                                              format="parquet")
                else:
                    output_path = "s3://" + output_bucket + "/" + key
                    source_df.write.format('parquet').save(output_path, mode='overwrite')
            except Exception as e:
                logger.error(f"Job parser failed while processing file path {str(key)} with error {e}")
                exception_error_paths.extend(f"parser failed while processing file path : {str(key)}")

    except Exception as e:
        logger.info(f"Job failed while running with the following error {e}")
        lambda_client = boto3.client('lambda', region_name='us-east-1')
        bytes_payload_msg = json.dumps(payload_msg).encode('utf-8')
        response = lambda_client.invoke(FunctionName=hook_lambda_function, Payload=bytes_payload_msg)
        logger.info(f"lambda trigger response : {response}")