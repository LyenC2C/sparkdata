import datetime
import sys
import os

# sys.argv[1]
exec_days = list(xrange(3, 44))
exec_days.reverse()

for i in exec_days:
    today = datetime.datetime.now()
    last_day = (today + datetime.timedelta(days=-i)).strftime('%Y%m%d')
    last_2_days = (today + datetime.timedelta(days=-i - 1)).strftime('%Y%m%d')
    # print exec_day,last_day
    command = "bash /home/wrt/sparkdata/lel/airflow/wrt/backfill/shopitem_c.sh {last_day} {last_2_days}".format(
        last_day=last_day, last_2_days=last_2_days)
    os.system(command)
