__author__ = 'zlj'
from pyspark.sql import *
from pyspark import SparkContext

import  datetime


sc = SparkContext(appName="shop-inc")

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


hiveContext.sql('use wlbase_dev')



def every_inc(x,ds):
    d0=datetime.datetime(2015, 1, 1)
    d2=datetime.datetime(2015, 10, 31)
    d={}
    for value in x:
        d[value[1]]=value[0]
    list=[]
    for i in xrange((d2-d0).days):
        before_d=d0+datetime.timedelta(days=i-1)
        next_d=d0+datetime.timedelta(days=i)
        before_ds=before_d.strftime("%Y%m%d")
        next_ds=next_d.strftime("%Y%m%d")
        keys= d.keys()
        if before_ds not in keys and next_ds not in keys:
            list.append((next_ds),0)
        elif before_ds not in keys and next_ds  in keys:
            list.append((next_ds,d[next_ds]))
        elif before_ds  in keys and next_ds not in keys:
            list.append((next_ds,d[next_ds]))
        elif before_ds  in keys and next_ds  in keys:
            list.append((next_ds,d[next_ds]-d[before_ds]))
        else:list.append((next_ds),0)




'''

select
item_id,total_sold,ds
from t_base_ec_item_sale_dev
where ds=

'''


rdd=hiveContext.sql('').map(lambda x:(x.item_id,(x.total_sold,x.ds))).groupByKey()
