#coding:utf-8
__author__ = 'wrt'
import sys
import copy
import math
import time
import datetime
import rapidjson as json
# import json
from pyspark import SparkContext

sc = SparkContext(appName="t_znk_record")

now_day = sys.argv[1]
# last_day = sys.argv[2]
y_now = int(now_day[0:4])
m_now = int(now_day[4:6])
d_now = int(now_day[6:8])
now_date = datetime.datetime(y_now,m_now,d_now)

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line):
    line = valid_jsontxt(line)
    ls = line.strip().split("\t")
    if len(ls) != 5:
        return [None]
    json_txt = ls[4][11:-1]
    ob = json.loads(json_txt)
    if type(ob) == type({}) and ob.has_key("data") and ob["data"].has_key("rateList"):
        data = ob['data']
        result = []
        for value in data['rateList']:
            lv = []
            itemid = value.get('auctionNumId', '-')
            feedid = value.get('id', '-')
            # userid = value.get('userId', '-')
            usermark = value.get('userMark','')
            dsn = value.get('feedbackDate', '-').replace("-","").replace(".","")
            y = int(dsn[0:4])
            m = int(dsn[4:6])
            d = int(dsn[6:8])
            dsn_date = datetime.datetime(y,m,d)
            # if itemid == "-" or feedid == "-" or userid == '-' or dsn == '-':
            feedback = value.get('feedback','-')
            if valid_jsontxt(feedback) == "评价方未及时做出评价,系统默认好评!":
                during_day = datetime.timedelta(days=22)
            elif valid_jsontxt(feedback) == "好评！":
                during_day = datetime.timedelta(days=11)
            else:
                during_day = datetime.timedelta(days=9)
            buy_date = dsn_date - during_day
            if int((now_date - buy_date).days) > 22:
                buy_day = str(buy_date)[:10].replace("-","")
                lv.append(feedid)
                lv.append(itemid)
                lv.append(usermark)
                lv.append(buy_day)
                result.append((feedid,lv))
            # result.append("\001".join([valid_jsontxt(i) for i in lv]))
        return result
    else:
        return [None]

def f2(line):
    ss = line.strip().split("\001")
    ss.append(last_day)
    return (ss[1],ss)

def twodays(x,y):   #同一个feedid下进行groupby后的结果
    item_list = y
    if len(item_list) == 1: #只有一个评论
        if len(item_list[0]) == 5:
            yes_item = item_list[0] #此评论为昨日评论（昨日评论多个ds字段），今日评论需要复制昨日评论
            result = yes_item[:-1]   #记得将最后的ds去掉，不要复制进来
        if len(item_list[0]) == 4:
            tod_item = item_list[0] #此评论为今日评论
            result = tod_item #使用默认值即可
    elif len(item_list) == 2: #有两个评论，一个是昨日，一个是今日
        #判断今日和昨日的位置并分别命名赋值
        if len(item_list[0]) == 5:
            tod_item = item_list[1]
        if len(item_list[0]) == 4:
            tod_item = item_list[0]
        result = tod_item
    else:
        result = ['-']
    return "\001".join([str(valid_jsontxt(i)) for i in result])




s1 = "/commit/tb_tmp/comments/" + now_day
# s2 = "/hive/warehouse/wlservice.db/t_wrt_znk_record/ds=" + last_day

rdd = sc.textFile(s1).flatMap(lambda x:f(x)).filter(lambda x:x!=None)\
    .groupByKey().mapValues(list).map(lambda (x,y):"\001".join(y[0]))
# rdd_last = sc.textFile(s2).map(lambda x:f2(x))
# rdd = rdd_now.union(rdd_last).groupByKey().mapValues(list).map(lambda (x, y):twodays(x, y)) #两天数据合并
rdd.saveAsTextFile('/user/wrt/temp/znk_feedmark_tmp')

# hfs -rmr /user/wrt/temp/znk_feedmark_tmp
# spark-submit  --executor-memory 1G  --driver-memory 5G  --total-executor-cores 80  t_wrt_znk_feedmark.py 20160919
# LOAD DATA  INPATH '/user/wrt/temp/znk_feedmark_tmp' OVERWRITE INTO TABLE t_wrt_znk_feedmark;

