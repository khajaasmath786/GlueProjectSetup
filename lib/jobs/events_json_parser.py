"""Module to parse EBP Events.
This script reads JSON records from zone1 and breaks the nested objects/arrays into tables as per the given schema,
if no schema given it will infer the schema from spark"""
import re
import sys
import json
import logging
import boto3
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from lib.utilities.z1_to_z2_json_parser.z1_to_z2_json_parser import JSONParser
from awsglue.job import Job
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

# read input parameters
args = getResolvedOptions(sys.argv,
                          ['input_bucket', 'JOB_NAME', 'output_path', 'schema_path', 'hook_function', 'WORKFLOW_NAME',
                           'WORKFLOW_RUN_ID'])
# args = getResolvedOptions(sys.argv, ['input_bucket', 'JOB_NAME', 'output_path', 'schema_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.caseSensitive", "true")
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


def __get_input_parameters():
    glue_client = boto3.client('glue')
    run_props = glue_client.get_workflow_run_properties(Name=args['WORKFLOW_NAME'], RunId=args['WORKFLOW_RUN_ID'])
    input_path = run_props['RunProperties']['inputPath']
    return input_path


def __get_file_name_and_received_date(file_path: str):
    """get file name & received date from file path"""
    file_name = file_path.split("/")[-1]
    received_date = "".join(file_path.split("/")[-5:-1])
    return file_name, received_date


if __name__ == "__main__":
    output_path = args['output_path']
    input_bucket = args['input_bucket']
    schema_path = args['schema_path']
    schema_bucket = schema_path.replace("s3://", "").split('/', 1)[0]
    schema_prefix = schema_path.replace("s3://", "").split('/', 1)[1]
    hook_function = args['hook_function']
    input_path_list = []
    schema_not_found_paths = []
    count_mismatch_paths = []
    exception_error_paths = []

    # Get input path list from Glue work flow
    # inputPath = "s3://ebp-dl-infra-dev-710183834284-use1-zone1/metadata/ebp_files_metadata/ebp-filemetadata20210711213030017013.json"
    logger.info("Get Input Path from Workflow")
    inputPath = __get_input_parameters()
    logger.info("Input Path from Workflow: " + inputPath)
    if len(inputPath) > 0:
        logger.info("Read Input Path to get list of s3 files to process")
        files_list = obj.load_json(inputPath)
        input_path_list = files_list['list_of_files']
        logger.info(f"List of s3 files to process: {input_path_list}")

    if len(input_path_list) > 0:
        # input & schema path
        common_input_path = f"s3://{input_bucket}/events/"
        common_schema_path = f"{schema_path}/"

        # get schema files
        schema_list = obj.get_all_s3_objects(schema_bucket, schema_prefix)
        schema_map = {}
        logger.info("Create Schema to file mapping")
        # map schema to input event file
        for event in input_path_list:
            event_type = event.split(common_input_path, 1)[1].split('/', 1)[0]
            common_schema_event_path = common_schema_path + event_type
            if common_schema_event_path + ".json" in schema_list:
                schema_map[event] = common_schema_event_path + ".json", \
                                    event_type.split('.', 1)[1].replace('.', '_')
            elif common_schema_path + event_type.rsplit('.', 1)[0] + ".json" in schema_list:
                schema_map[event] = \
                    common_schema_event_path.rsplit('.', 1)[0] + ".json", \
                    (event_type.rsplit('.', 1)[0]).split('.', 1)[1].replace('.', '_')
            else:
                logger.error(f"schema not found in given list of schemas")
                schema_not_found_paths.append(event)

        logger.info("Schema to file mapping is completed")

        # UDF & UDF Schema to return filename and received date
        udf_schema = StructType([
            StructField("input_file_name", StringType(), False),
            StructField("received_date", StringType(), False)])
        get_file_name_date_udf = F.udf(lambda z: __get_file_name_and_received_date(z), udf_schema)

        # Parse one event type at a time
        for input_path, schema_and_root in schema_map.items():
            try:
                logger.info("Start creating a dataframe")
                parsed_df, source_count_df = obj.parse_json_files(spark, schema_and_root[0], input_path)
                # Get input filename & received date
                logger.info("Add Input filename and received date to the dataframe")
                parsed_df = parsed_df.withColumn("input_file_path", F.input_file_name())
                parsed_df = parsed_df.withColumn("file_name_received_date",
                                                 F.explode(F.array(get_file_name_date_udf(F.col("input_file_path")))))

                parsed_df = parsed_df.select('*', "file_name_received_date.*").drop("file_name_received_date")

                logger.info("validate source and target counts for parsed dataframes")
                parsed_count_df = parsed_df.groupBy(F.col("input_file_path")).count()
                logger.info(f"source count is {source_count_df.show(100, False)}")
                logger.info(f"Target count for parsed df is {parsed_count_df.show(100, False)}")
                # If mismatch found remove problem file records from dataframe before converting it to table dataframes
                if source_count_df.union(parsed_count_df).subtract(
                        source_count_df.intersect(parsed_count_df)).count() != 0:
                    logger.info("Collect problem file names when source count doesnot match with target count")
                    df_collect = source_count_df.union(parsed_count_df).subtract(
                        source_count_df.intersect(parsed_count_df)).select("input_file_path").collect()
                    count_mismatch_paths.extend([row.input_file_path for row in df_collect])
                logger.info(
                    f"Records mismatch found in files before converting to tables: {list(set(count_mismatch_paths))}")
                logger.info("Remove the files that were not parsed properly before converting to tables")
                parsed_df = parsed_df.filter(~F.col("input_file_path").isin(count_mismatch_paths)).drop(
                    F.col("input_file_path"))
                # Convert to tables
                logger.info("Start converting to tables")
                tables = obj.convert_to_tables(parsed_df, schema_and_root[1])
                logger.info("Convertion to tables completed")

                # Filter out error files add partiotions and write to S3
                for table in tables:
                    table_name, table_df = table
                    # remove special characters and white spaces from column names
                    logger.info(f"Create a df and write it to S3 for {table_name} table")
                    logger.info("Remove special characters and white spaces from column names")
                    table_df = table_df.toDF(*[re.sub(r"[\)|\(|\s|,|%]", '', x) for x in table_df.columns])
                    logger.info(f"Filter files that are failed to process before writing to S3")
                    table_df = table_df.filter(~F.col("input_file_name").isin(count_mismatch_paths))
                    # split date to year, month, day and hour to create hourly partitions
                    logger.info("Adding Year, Month, Day and Hour columns as partition columns")
                    table_df = table_df.withColumn("received_date",
                                                   F.to_timestamp(F.col("received_date"), 'yyyyMMddHH')). \
                        withColumn("year", F.year(F.col("received_date"))). \
                        withColumn("month", F.month(F.col("received_date"))). \
                        withColumn("day", F.dayofmonth(F.col("received_date"))). \
                        withColumn("hour", F.hour(F.col("received_date")))
                    logger.info(f"Write {table_name} table dataframe to S3 location {output_path}/{table_name}")
                    table_df.write.mode("append").partitionBy("year", "month", "day", "hour"). \
                        parquet(output_path + "/" + table_name)
            except Exception as e:
                logger.error(f"Job parser failed while processing file path {input_path} with error {e}")
                exception_error_paths.extend(f"parser failed while processing file path : {input_path}")
            # Raise hook alert when we see files that are not processed properly
        if len(count_mismatch_paths) | len(exception_error_paths) | len(schema_not_found_paths) > 0:
            payload_msg = {"detail": {"WorkflowName": args['WORKFLOW_NAME'],
                                      "WorkflowRunId": args['WORKFLOW_RUN_ID'],
                                      "jobName": args['JOB_NAME'],
                                      "failure_message": f"List of files failed to process: {list(set(schema_not_found_paths))} \
                                            {list(set(exception_error_paths))} {list(set(count_mismatch_paths))}"}}
            logger.info(f"Trigger lambda to Raise a Hook Alert with following error: {payload_msg}")
            bytes_payload_msg = json.dumps(payload_msg).encode('utf-8')
            lambda_client = boto3.client('lambda', region_name='us-east-1')
            response = lambda_client.invoke(FunctionName=hook_function, Payload=bytes_payload_msg)
            logger.info(f"lambda trigger response : {response}")