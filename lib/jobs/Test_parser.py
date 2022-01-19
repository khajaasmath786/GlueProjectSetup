"""unit tests for ebp_z1_to_z2_job_stack"""
from unittest import TestCase
import json
from aws_cdk import core
from moo_cdk.core import EnvironmentName
from lib.stacks.jobs.glue_job_stack import GlueJobStack


def synth_with_schema_template():
    """generate the template that is in cdk.out"""
    app = core.App()
    GlueJobStack(app, 'Z1toZ2ParsingJobTestStack1',
                 asi_name="ebp",
                 environment_name=EnvironmentName.DEV,
                 project_name="ebp-events-dp",
                 parameters={"script_path":"./lib/scripts/jobs/ebp_events_z1_to_z2_parser.py",
                             "s3_resources":["arn:aws:s3:::zone1_bucket_name/*",
                                             "arn:aws:s3:::zone2_bucket_name/*"],
                             "default_args":{
                                 "--enable-continuous-cloudwatch-log": True,
                                 "--job-language": "python",
                                 "--enable-glue-datacatalog": True,
                                 "--output_path": "s3://zone2_bucket_name",
                                 "--input_bucket": "zone1_bucket_name",
                                 "--schema_path": "s3://zone1_bucket_name/eventschemas/event_schemas/",
                                 "--input_path_list": "[s3://zone1_bucket_name/Contact_Trace_Records/*/*/*/*]",
                                 "--hook_function": "function_name"},
                             "extra_files_path": "./lib/utilities/z1_to_z2_json_parser",
                             "naming_suffix": "parse_ebp_events_z1_to_z2",
                             "zone1_bucket": "zone1_bucket_name",
                             "schema_files_path":"./lib/utilities/event_schemas",
                             "hook_function": "function_name",
                             "schema_bucket_prefix":"eventschemas/event_schemas"})
    data = app.synth().get_stack("Z1toZ2ParsingJobTestStack1").template
    return json.dumps(data)

def synth_without_schema_template():
    """generate the template that is in cdk.out"""
    app = core.App()
    GlueJobStack(app, 'Z1toZ2ParsingJobTestStack2',
                 asi_name="ebp",
                 environment_name=EnvironmentName.DEV,
                 project_name="ebp-events-dp",
                 parameters={"script_path":"./lib/scripts/jobs/ebp_events_z1_to_z2_parser.py",
                             "s3_resources":["arn:aws:s3:::zone1_bucket_name/*",
                                             "arn:aws:s3:::zone2_bucket_name/*"],
                             "default_args":{
                                 "--enable-continuous-cloudwatch-log": True,
                                 "--job-language": "python",
                                 "--enable-glue-datacatalog": True,
                                 "--output_path": "s3://zone2_bucket_name",
                                 "--input_bucket": "zone1_bucket_name",
                                 "--schema_path": "s3://zone1_bucket_name/eventschemas/ebp_events-eventschemas/ebp_events_schema.json",
                                 "--input_path_list": "[s3://zone1_bucket_name/Contact_Trace_Records/*/*/*/*]",
                                 "--hook_function": "function_name"},
                             "naming_suffix":"parse_ebp_events_z1_to_z2",
                             "hook_function": "function_name",
                             "zone1_bucket": "zone1_bucket_name"})
    data = app.synth().get_stack("Z1toZ2ParsingJobTestStack2").template
    return json.dumps(data)



class TestGlueJobStack(TestCase):
    """unit tests for lib.stacks.jobs.glue_job_stack"""
    def setUp(self) -> None:
        self.first_template = synth_with_schema_template()
        self.second_template = synth_without_schema_template()

    def test_resources_created(self):
        """Testing that Glue Job and LakeFormation Permissions are in template"""
        self.assertIn("AWS::Glue::Job", self.first_template)
        self.assertIn("AWS::LakeFormation::Permissions", self.first_template)
        self.assertIn("AWS::Glue::Job", self.second_template)
        self.assertIn("AWS::LakeFormation::Permissions", self.second_template)
        self.assertIn("Custom::CDKBucketDeployment", self.first_template)
        self.assertNotIn("Custom::CDKBucketDeployment", self.second_template)