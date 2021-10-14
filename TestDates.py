import sys
import datetime
import calendar
import os
os.system("c:\python39\python.exe -m pip install --upgrade pip")
os.system("pip install dateutil")
from dateutil.relativedelta import relativedelta
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
# if we want run manually we need to disable the below command
import sys
import calendar
from datetime import date
import sys
import calendar
from datetime import date

MONTHS_BACK_START = sys.argv[1]
MONTHS_BACK_END = sys.argv[2]

MONTHS_BACK_START = int(MONTHS_BACK_START)
MONTHS_BACK_END = int(MONTHS_BACK_END)


def monthdelta(date, MONTHS_BACK_START):
    m, y = (date.month + MONTHS_BACK_START) % 12, date.year + ((date.month) + MONTHS_BACK_START - 1) // 12
    if not m: m = 12
    d = min(date.day, calendar.monthrange(y, m)[1])
    return date.replace(day=d, month=m, year=y)


if MONTHS_BACK_START >= MONTHS_BACK_END:
    print("Start date is greater then end date")

else:
    print("Start date is less then end date")

    sys.exit()

start_month = monthdelta(date.today(), -MONTHS_BACK_START)

end_month = monthdelta(date.today(), -MONTHS_BACK_END)

first_day_of_month = start_month.replace(day=1)

last_day_of_month = end_month.replace(day=calendar.monthrange(end_month.year, end_month.month)[1])

print("\nFirst day of month: ", first_day_of_month, "\n")

print("\nLast day of month: ", last_day_of_month, "\n")

# First day of month:  2021-08-01
# Last day of month:  2021-08-31
#First day of month:  2021-08-01# Last day of month:  2021-08-31