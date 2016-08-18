#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkConf
import rapidjson as json
conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
conf.set("spark.kryoserializer.buffer.mb", "1024")
conf.set("spark.akka.frameSize", "100")
conf.set("spark.network.timeout", "1000s")
conf.set("spark.driver.maxResultSize", "8g")

sc = SparkContext(appName="user_cattags", conf=conf)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
        return res
    else: return res
# coding=utf8
# import json
import sys
from email  import utils
import re

from datetime import datetime




def fun(line):
    if "\001" not in line :return None
    # ls= valid_jsontxt(line).split("\001")
    # if len(ls)!=2:return None
    mid, s = line.split("\001")

    j = json.loads(valid_jsontxt(s))
    if type(j)!=type({}):return None

    rs=[]
    for wb in j['reposts']:
        if 'retweeted_status' not in wb:
            continue

        date_tz = utils.parsedate_tz(wb['created_at'])
        end_date = datetime(*date_tz[:6])

        date_tz = utils.parsedate_tz(
            wb['retweeted_status']['created_at'])
        start_date = datetime(*date_tz[:6])
        if start_date.year<2015:continue

        t = int((end_date - start_date).total_seconds())

        #wstr1 = '\001'.join([mid, wb['retweeted_status']['user']['idstr'], wb['user']['idstr'], str(t)])

        name = wb['retweeted_status']['user']['screen_name']
        # re_s = re.findall(u'//@(.*?)[:：]{1}', wb['text'], re.UNICODE)
        # if len(re_s) != 0:
        #     name = re_s[0]
        # wstr1 = '\001'.join(
        #     [mid, name.encode('utf8'), wb['user']['screen_name'].encode('utf8'), str(t)])
        # wf_1.write(wstr1 + '\n')

        txt = valid_jsontxt(wb['text']).replace('转发微博', '').replace('\n', '') ##.split('//@')[0]
        if '//@' in txt:
            next_user_name=valid_jsontxt(txt).split('//@')[1].split(':')[0]
        else: next_user_name=""
        wstr2 = '\001'.join( [valid_jsontxt(i) for i in [mid,
                                                         name,wb['retweeted_status']['user']['idstr'],
                                                         wb['user']['screen_name'],wb['user']['idstr'],
                                                         next_user_name ,
                                                         str(t), txt]])
        rs.append(wstr2)

    return  rs

def try_fun(line):
    try:
        return fun(line)
    except: return None
sc.textFile('/commit/weibo_dc').map(lambda x:try_fun(x)).filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/commit/weibo_dc_parse2015')
# sc.textFile('/user/zlj/tmp/1').map(lambda x:fun(x)).filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/user/zlj/tmp/1_parse')


# wf_2.close()
# wf_1.close()
