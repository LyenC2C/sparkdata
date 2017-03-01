__author__ = 'wrt'
import sys
from pyspark import SparkContext
yesterday = sys.argv[1]
today = sys.argv[2]

sc = SparkContext(appName="cal_daysale " + yesterday)
s1 = "/hive/warehouse/wl_base.db/t_base_ec_item_sold_dev/ds=" + yesterday
s2 = "/hive/warehouse/wl_base.db/t_base_ec_item_sold_dev/ds=" + today

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content
def yes_sale(line):
    ss = line.strip().split('\001')
    itemid = ss[0]
    if not ss[1].replace(".","").isdigit(): ss[1] = 0.0
    item_price = float(ss[1])
    if not ss[3].isdigit(): ss[3] = 0
    item_sale = int(ss[3])
    flag = "yes"
    return (itemid,[item_price,item_sale,flag])

def tod_sale(line):
    ss = line.strip().split('\001')
    itemid = ss[0]
    if not ss[1].replace(".","").isdigit(): ss[1] = 0.0
    item_price = float(ss[1])
    if not ss[3].isdigit(): ss[3] = 0
    item_sale = int(ss[3])
    flag = "tod"
    return (itemid,[item_price,item_sale,flag])

def cal(x,y):
    twodays = y
    # if len(twodays) == 1:
        # info_list = twodaysxueyaddd
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
    if day_sold == 0:
        return None
    else:
        return str(valid_jsontxt(x)) + "\001" + str(day_sold) + "\001" + str(day_sold_price)

# rdd1 = textFile(s1).map(lambda x:(x.split('\001')[0],x.split('\001')[1:]))
# rdd2 = textFile(s2).map(lambda x:(x.split('\001')[0],x.split('\001')[1:]))
rdd1 = sc.textFile(s1).map(lambda x:yes_sale(x))
rdd2 = sc.textFile(s2).map(lambda x:tod_sale(x))
rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x,y):cal(x,y)).filter(lambda x:x!=None)
rdd.coalesce(200).saveAsTextFile('/user/wrt/daysale_tmp')
