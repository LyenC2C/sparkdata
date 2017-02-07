import datetime
import sys
import os

# sys.argv[1]
exec_days = list(xrange(1, 4))
exec_days.reverse()

for i in exec_days:
    today = datetime.datetime.now()
    exec_day = (today + datetime.timedelta(days=-i)).strftime('%Y%m%d')
    last_day = (today + datetime.timedelta(days=-i-1)).strftime('%Y%m%d')
    last_2_days =  (today + datetime.timedelta(days=-i-2)).strftime('%Y%m%d')
    # print exec_day,last_day,last_2_days
    command = "bash /home/wrt/sparkdata/lel/airflow/wrt/backfill/shopitem_c_backfill.sh {last_day} {last_2_days}".format(
        last_day=last_day, last_2_days=last_2_days)
    os.system(command)
