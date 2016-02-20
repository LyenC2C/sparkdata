__author__ = 'wrt'
import sys
from pyspark import SparkContext
yesterday = sys.argv[1]
today = sys.argv[2]

sc = SparkContext(appName="cal_daysale " + yesterday)
s1 = "/user/wrt/itemsale/ds=" + yesterday
s2 = "/user/wrt/itemsale/ds=" + today

def yes_sale(line):
    ss = line.strip().split('\001')
    itemid = ss[0]
    item_price = float(ss[3])
    item_sale = int(ss[6])
    flag = "yes"
    return (itemid,[item_price,item_sale,flag])

def tod_sale(line):
    ss = line.strip().split('\001')
    itemid = ss[0]
    item_price = float(ss[3])
    item_sale = int(ss[6])
    flag = "tod"
    return (itemid,[item_price,item_sale,flag])

def cal(x,y):
    twodays = y
    if len(twodays) == 1:
        # info_list = twodays
        # if info_list[2] == "yes":
        day_sold = 0
        day_sold_price = 0.0
        # if info_list[2] == "tod":
        #     day_sold = 0
    if len(twodays) == 2:
        if twodays[0][2] == "yes":
            yesterday_info = twodays[0]
            today_info = twodays[1]
        if twodays[0][2] == "tod":
            yesterday_info = twodays[1]
            today_info = twodays[0]
        day_sold = max(today_info[1] - yesterday_info[1], 0)
        day_sold_price = float(day_sold) * yesterday_info[0]
    return str(x) + "\001" + str(day_sold) + "\001" + str(day_sold_price)

# rdd1 = textFile(s1).map(lambda x:(x.split('\001')[0],x.split('\001')[1:]))
# rdd2 = textFile(s2).map(lambda x:(x.split('\001')[0],x.split('\001')[1:]))
rdd1 = textFile(s1).map(lambda x:yes_sale(x))
rdd2 = textFile(s2).map(lambda x:tod_sale(x))
rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x,y):cal(x,y))
rdd.saveAsTextFile('/user/wrt/daysale/ds=' + yesterday)