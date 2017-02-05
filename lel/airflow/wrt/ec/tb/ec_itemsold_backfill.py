import datetime
import sys
import os

# sys.argv[1]
exec_days = list(xrange(0,6))
exec_days.reverse()
iteminfo_date = 20170128

for i in exec_days:
    today = datetime.datetime.now()
    exec_day = (today + datetime.timedelta(days=-i-1)).strftime('%Y%m%d')
    last_day = (today + datetime.timedelta(days=-i-2)).strftime('%Y%m%d')
    last_2_days =  (today + datetime.timedelta(days=-i-3)).strftime('%Y%m%d')
    # print exec_day,last_day,last_2_days
    command = "bash /home/wrt/sparkdata/lel/airflow/wrt/ec/tb/ec_itemsold_backfill.sh  {last_day} {last_2_days} {iteminfo_date}".format(last_day=last_day,last_2_days=last_2_days,iteminfo_date=iteminfo_date)
    os.system(command)
