"""This is a parser code to parse JSON files into Structured format"""
import sys
import json
import logging
import boto3
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, \
    FloatType, BooleanType, StructType, ArrayType, DateType, TimestampType,DecimalType
from pyspark.sql import SparkSession



class JSONParser():
    """Class to parse json files with or without schema"""

    def __init__(self, logger):
        self.logger = logger

    s3_client = boto3.client('s3')
    df_type_map = {"string": StringType,
                   "ARN": StringType,
                   "number": DoubleType,
                   "float": FloatType,
                   "integer": IntegerType,
                   "boolean": BooleanType,
                   "object": StructType,
                   "array": ArrayType,
                   "date": DateType,
                   "decimal": DecimalType,
                   "date-time": TimestampType,
                   "None": None}

    @staticmethod
    def __rename_duplicate_columns(dup_df):
        """rename duplicate columns by appending a count to it"""
        renamed_cols = []
        counter = {col.lower(): -1 for col in dup_df.columns}
        for col in dup_df.columns:
            counter[col.lower()] += 1
            col += str(counter[col.lower()]) if counter[col.lower()] else ""
            renamed_cols.append(col)
        return renamed_cols

    @staticmethod
    def __get_flat_cols(temp_df):
        """Get Flat Cols to create tables"""
        flat_cols = [d_type[0] for d_type in temp_df.dtypes if \
                     not d_type[1].startswith("array") and not d_type[1].startswith("struct")]
        return flat_cols

    @staticmethod
    def __generate_uuid(temp_df):
        """Generate UUID for nested and array objects"""
        for d_type in temp_df.dtypes:
            if d_type[1].startswith("array"):
                temp_df = temp_df.withColumn(d_type[0] + "_UUID", F.when(F.size(d_type[0]) > 0, F.expr('uuid()')). \
                                             otherwise(None))
            if d_type[1].startswith("struct"):
                temp_df = temp_df.withColumn(d_type[0] + "_UUID", F.when(F.col(d_type[0]).isNotNull(), \
                                                                         F.expr('uuid()')).otherwise(None))
        return temp_df

    def convert_to_tables(self, nested_df, root_defn):
        """Convert nested dataframes to Tables"""
        stack = [(root_defn, nested_df)]
        tables = []

        while stack:
            parents, temp_df = stack.pop()
            columns = []
            distinct_cols = self.__rename_duplicate_columns(temp_df)
            temp_df = temp_df.toDF(*distinct_cols)
            temp_df = self.__generate_uuid(temp_df)
            flat_cols = self.__get_flat_cols(temp_df)
            nested_cols = [d_type[0] for d_type in temp_df.dtypes if d_type[1].startswith("struct")]
            array_cols = [d_type[0] for d_type in temp_df.dtypes if d_type[1].startswith("array")]
            uuid_cols = [d_type for d_type in flat_cols if d_type.find('_UUID') > 0]
            columns.extend(flat_cols)

            for a_col in array_cols:
                new_df = temp_df.select(F.col("input_file_name"), F.col("received_date"), F.col(a_col + "_UUID"),
                                        F.posexplode(a_col).alias("index", a_col))
                new_df = self.__unnest_struct_column(new_df)
                stack.append((parents + "_" + a_col, new_df))

            for a_col in nested_cols:
                new_df = temp_df.select(F.col("input_file_name"),
                                        F.col("received_date"),
                                        F.col(a_col + "_UUID"),
                                        a_col + ".*").filter(F.col(a_col + "_UUID").isNotNull())
                stack.append((parents + "_" + a_col, new_df))

            if len(columns) > 0 and set(columns) != set(uuid_cols):
                tables.append((parents, temp_df.select(columns)))
        return tables

    @staticmethod
    def __unnest_struct_column(nested_df):
        """Unnest struct columns (nested objects)"""
        flat_cols = [c[0] for c in nested_df.dtypes if not c[1].startswith("array") and not c[1].startswith("struct")]
        nested_cols = [c[0] for c in nested_df.dtypes if c[1].startswith("struct")]
        for nested_col in nested_cols:
            un_nested_df = nested_df.select(nested_col + ".*")
            flat_cols.extend([F.col(nested_col + "." + c[0]).alias(c[0]) for c in un_nested_df.dtypes])
        unnest_df = nested_df.select(flat_cols)
        return unnest_df

    def get_all_s3_objects(self, bucket_name: str, prefix_name: str):
        """Get all S3 objects with list_objects_v2 api call"""
        continuation_token = None
        s3_input_files_list = []
        while True:
            args = dict(Bucket=bucket_name, Prefix=prefix_name, MaxKeys=1000)
            if continuation_token:
                args['ContinuationToken'] = continuation_token
            object_summaries = self.s3_client.list_objects_v2(**args)
            list_of_objects = object_summaries.get('Contents', [])
            for object in list_of_objects:
                if not object.get('Key')[-1] == '/':
                    key = "s3://" + bucket_name + "/" + object.get('Key')
                    s3_input_files_list.append(key)
            if not object_summaries.get('IsTruncated'):
                break
            continuation_token = object_summaries.get('NextContinuationToken')
        return s3_input_files_list

    def __merge_schema(self, master_schema, delta_schema):
        """Merge schema to capture any additional fields present in data but not in schema json"""
        if isinstance(delta_schema, StringType) and isinstance(master_schema, StructType):
            delta_schema = StructType()
        master_schema_fields = master_schema.fields if isinstance(master_schema, StructType) else []
        delta_schema_fields = delta_schema.fields if isinstance(delta_schema, StructType) else []
        delta_fields = [d_field.name for d_field in delta_schema_fields]
        for m_field in master_schema_fields:
            if m_field.name in delta_fields:
                if isinstance(m_field.dataType, StructType) and \
                        isinstance(delta_schema.__getitem__(m_field.name).dataType, StructType):
                    self.__merge_schema(master_schema.__getitem__(m_field.name).dataType,
                                        delta_schema.__getitem__(m_field.name).dataType)
                elif isinstance(m_field.dataType, ArrayType) and \
                        isinstance(delta_schema.__getitem__(m_field.name).dataType, ArrayType):
                    self.__merge_schema(master_schema.__getitem__(m_field.name).dataType.elementType,
                                        delta_schema.__getitem__(m_field.name).dataType.elementType)
                else:
                    delta_schema.__getitem__(m_field.name).dataType = m_field.dataType
            else:
                delta_schema.add(m_field.name, m_field.dataType, True)
        return delta_schema

    def load_json(self, file_url: str):
        """Load json formatted schema file"""
        bucket_name = file_url.replace("s3://", "").split('/', 1)[0]
        object_name = file_url.replace("s3://", "").split('/', 1)[1]
        self.logger.info("load JSON File")
        object_body = self.s3_client.get_object(Bucket=bucket_name, Key=object_name)
        object_content = object_body['Body'].read().decode('utf-8')
        json_schema = json.loads(str(object_content))
        return json_schema

    def __build_df_schema(self, main_prop: json, delta_json: json):
        """Build dataframe schema from JSON schema"""
        struct = StructType()
        if len(main_prop) == 0:
            return struct
        for item in delta_json['properties'].items():
            key, value = item
            if value.get('type') == 'string' and value.get('format') is not None:
                struct.add(key, self.df_type_map[value.get('format')](), True)
            elif value.get('type') is not None and value.get('type') != "array":
                struct.add(key, self.df_type_map[value.get('type')](), True)
            elif value.get('type') == "array" and (value.get('items')).get('type') is not None:
                struct.add(key, ArrayType(self.df_type_map[(value.get('items')).get('type')]()), True)
            elif value.get('type') == "array" and (value.get('items')).get('$ref') is not None:
                struct.add(key, ArrayType(self.__build_df_schema(main_prop, (main_prop.get('definitions')).get(
                    (value.get('items')).get('$ref').split("/")[-1]))), True)
            elif value.get('$ref') is not None and value.get('type') is None:
                struct.add(key,
                           self.__build_df_schema(main_prop, (main_prop.get('definitions')). \
                                                  get(value.get('$ref').split("/")[-1])), True)
        return struct

    def parse_json_files(self, spark: SparkSession, schema_path: str, input_path: str):
        """Parse the JSON files by applying json formatted schema"""
        if schema_path.strip() == "" or schema_path is None:
            # if no schema provided just infer schema from data
            self.logger.info("Infer schema from data, no Schema provided")
            parsed_df = spark.read.option("inferSchema", "true") \
                .json(input_path)
            parsed_df = parsed_df.withColumn("input_file_path", F.input_file_name())
            self.logger.info("get source count for inferred dataframe")
            source_count_df = parsed_df.groupBy(F.col("input_file_path")).count()
            self.logger.info(f"source count for inferred dataframe: {source_count_df.show(100, False)}")
        else:
            self.logger.info("Infer schema from Schema provided")
            schema_json = self.load_json(schema_path)
            self.logger.info(schema_json)
            # build dataframe schema and an empty dataframe
            self.logger.info("Build Dataframe Schema started")
            master_schema_df = spark.createDataFrame([], self.__build_df_schema(schema_json, schema_json))
            # read data to infer schema
            self.logger.info("Build Dataframe Schema Completed")
            self.logger.info("Infer schema from data to merge later")
            # inferred_df = spark.read.json(input_path)
            inferred_df = spark.read.option("inferSchema", "true") \
                .json(input_path)
            self.logger.info("get source count for inferred dataframe")
            inferred_df = inferred_df.withColumn("input_file_path", F.input_file_name())
            source_count_df = inferred_df.groupBy(F.col("input_file_path")).count()
            self.logger.info(f"source count for inferred dataframe: {source_count_df}")
            # Merge Schema
            self.logger.info("Merge Inferred schema with Json schema")
            merged_schema_struct = self.__merge_schema(master_schema_df.schema, inferred_df.schema)
            self.logger.info("Schema merge completed")
            # Create new DF by enforcing the converted Schema
            self.logger.info("Create a parsed Dataframe by applying the merged schema")
            parsed_df = spark.read.option("inferSchema", "true") \
                .schema(merged_schema_struct) \
                .json(input_path)

        return parsed_df, source_count_df