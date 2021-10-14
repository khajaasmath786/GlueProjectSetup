import sys
import datetime
import calendar
import os
os.system("c:\python39\python.exe -m pip install --upgrade pip")
os.system("pip install dateutil")
from dateutil.relativedelta import relativedelta

# if we want run manually we need to disable the below command
MONTHS_BACK_START = sys.argv[1]
MONTHS_BACK_END = sys.argv[2]

if MONTHS_BACK_START >= MONTHS_BACK_END:
    print("Start date is greater then end date")

else:
    print("Start date is less then end date")

    sys.exit()

MONTHS_BACK_START = int(MONTHS_BACK_START)
MONTHS_BACK_END = int(MONTHS_BACK_END)

# if we want run manually we need to enable the below commands
# startday=5
# endday=2

FirstDay = datetime.date.today()

LastDay = datetime.date.today()

first_day_of_month = FirstDay.replace(day=1) + relativedelta(months=-MONTHS_BACK_START)

last_day_of_month = LastDay.replace(day=calendar.monthrange(LastDay.year, LastDay.month)[1]) + relativedelta(
    months=-MONTHS_BACK_END)

print("\nFirst day of month: ", first_day_of_month, "\n")

print("\nLast day of month: ", last_day_of_month, "\n")

# First day of month:  2021-08-01
# Last day of month:  2021-08-31