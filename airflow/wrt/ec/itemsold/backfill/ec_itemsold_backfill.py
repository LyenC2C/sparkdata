#!coding=utf-8
import datetime
import sys
import os

'''
:param 0~4
first param plus 1 day 1~4 for airflow.sql to execute
'''
def backfill_continuously(latest_day, days,iteminfo_date):
    exec_days = list(xrange(latest_day, days))
    exec_days.reverse()
    for i in exec_days:
        today = datetime.datetime.now()
        exec_day = (today + datetime.timedelta(days=-i)).strftime('%Y%m%d')
        last_day = (today + datetime.timedelta(days=-i - 1)).strftime('%Y%m%d')
        last_2_days = (today + datetime.timedelta(days=-i - 2)).strftime('%Y%m%d')
        # print exec_day, last_day, last_2_days
        command =" bash /home/wrt/sparkdata/airflow.sql/wrt/ec/itemsold/backfill/ec_itemsold_backfill.sh  {last_day} {last_2_days} {iteminfo_date}".format(last_day=last_day,last_2_days=last_2_days,iteminfo_date=iteminfo_date)
        os.system(command)

def backfill_individually(last_day, last_update_day,iteminfo_date):
    command = "bash /home/wrt/sparkdata/airflow.sql/wrt/ec/itemsold/backfill/ec_itemsold_backfill.sh  {last_day} {last_update_day} {iteminfo_date}".format(last_day=last_day,last_update_day=last_update_day,iteminfo_date=iteminfo_date)
    # print last_day,last_update_day,iteminfo_date
    os.system(command)



'''
:param 0~4
first param plus 1 day like 1~4 for airflow.sql to execute
second param subtract 1 day like 1~3 for tolerancing
'''
def backfill_allkindsof(last_day, last_update_day, latest_day, days,iteminfo_date):
    backfill_individually(20170417, 20170407,20170418)
    backfill_continuously(1,3,20170421)


# backfill_continuously(1,3,20170421)
# backfill_individually(20170417, 20170407,20170418)
