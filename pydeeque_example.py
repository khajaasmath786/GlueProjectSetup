from pyspark.sql import SparkSession, Row
import pydeequ
from pydeequ.suggestions import *
from pydeequ.analyzers import *
from pydeequ.profiles import *
from pydeequ.checks import *
from pydeequ.verification import *

from pydeequ.scala_utils import ScalaFunction1, to_scala_seq
## Creating Spark Session
spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())

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

##Automated constraint suggestion
# Output: "code_for_constraint": ".isComplete(\"emp_age\")" --> This will give what constraint you can use on that dataset
suggestionResult = ConstraintSuggestionRunner(spark) \
             .onData(df) \
             .addConstraintRule(DEFAULT()) \
             .run()
# Constraint Suggestions in JSON format
#print(json.dumps(suggestionResult, indent=2))

for sugg in suggestionResult['constraint_suggestions']:
    print(f"Constraint suggestion for \'{sugg['column_name']}\': {sugg['description']}")
    print(f"The corresponding Python code is: {sugg['code_for_constraint']}\n")
### Constraints Checks
###'''Below example is cheking on completeness, uniqueness,allowed values, email pattern, datatype'''
check = Check(spark, CheckLevel.Warning, "Review Check")

checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check.hasMin("emp_sal", lambda x: x >= 0) \
        .hasMax("emp_age", lambda x: x <= 100) \
        .hasSize(lambda x: x >= 5) \
        .isComplete("emp_sal")  \
        .isUnique("emp_name")  \
        .isContainedIn("emp_dept", ["Social", "Science", "Maths"]) \
        .isNonNegative("emp_age") \
        .containsEmail("emp_email") \
        ).run()

checkResult_ds = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_ds.show(truncate=False)

if checkResult.status == "Success":
    print('The data passed the test, everything is fine!')

else:
    print('We found errors in the data, the following constraints were not satisfied:')

    for check_json in checkResult.checkResults:
        if check_json['constraint_status'] != "Success":
            print(f"\t{check_json['constraint']} failed because: {check_json['constraint_message']}")

checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check.hasMin("emp_sal", lambda x: x == 0) \
        .isComplete("emp_sal")  \
        .isUnique("emp_name")  \
        .isContainedIn("emp_dept", ["Social", "Science", "Maths"]) \
        .isNonNegative("emp_age") \
        .containsEmail("emp_email") \
        .hasDataType("emp_age",ConstrainableDataTypes.Integral,assertion=lambda x: x == 1.0) \
        #.hasPattern(column='emp_email',pattern=r"""(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])""",
         #            assertion=lambda x: x == 1.0)) \
        ).run()

### Converting the result to dataframe
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show(truncate=False)


## Analzises the data for examples Completeness,Distinctness,Datatype etc...
analysisResult = AnalysisRunner(spark) \
                    .onData(df) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("emp_sal")) \
                    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
analysisResult_df.show(truncate=False)



### Complete Profile on dataframe - Column level analysis is provided by this method
### Generally runs on Numeric datatypes . In this case emp_age
result = ColumnProfilerRunner(spark) \
    .onData(df) \
    .run()

### Looping through the result object and printing the profile of each column
for col, profile in result.profiles.items():
    print(profile)

spark.sparkContext._gateway.shutdown_callback_server()
spark.stop()
