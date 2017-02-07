import datetime
import sys
import os

# sys.argv[1]
exec_days = list(xrange(1,3))
exec_days.reverse()

for i in exec_days:
    today = datetime.datetime.now()
    exec_day = (today + datetime.timedelta(days=-i)).strftime('%Y%m%d')
    last_day = (today + datetime.timedelta(days=-i-1)).strftime('%Y%m%d')
    last_2_days =  (today + datetime.timedelta(days=-i-2)).strftime('%Y%m%d')
    # print exec_day,last_day,last_2_days
    command = "bash /home/wrt/sparkdata/lel/airflow/wrt/ec/tb/ec_shopitem_b_backfill.sh {last_day} {last_2_days}".format(last_day=20170204,last_2_days=20170124)
    os.system(command)
