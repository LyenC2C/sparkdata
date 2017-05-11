import os
import datetime
def get_lastday():
    lastday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    return str(lastday)
listfile=os.listdir("/home/lyen/sh")
for i in filter(lambda a: get_lastday() in a,listfile):
    print i