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
        return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    else: return res
# coding=utf8
# import json
import sys
import email.utils
import re
from datetime import datetime




def fun(line):
    if '\001' not in line :return None
    mid, s = valid_jsontxt(line).split('\001')[:1]
    j = json.loads(s)
    if type(j)!=type({}):return None

    for wb in j['reposts']:
        if 'retweeted_status' not in wb:
            continue

        date_tz = email.utils.parsedate_tz(wb['created_at'])
        end_date = datetime(*date_tz[:6])

        date_tz = email.utils.parsedate_tz(
            wb['retweeted_status']['created_at'])
        start_date = datetime(*date_tz[:6])

        t = int((end_date - start_date).total_seconds())

        #wstr1 = '\001'.join([mid, wb['retweeted_status']['user']['idstr'], wb['user']['idstr'], str(t)])

        name = wb['retweeted_status']['user']['screen_name']
        # re_s = re.findall(u'//@(.*?)[:：]{1}', wb['text'], re.UNICODE)
        # if len(re_s) != 0:
        #     name = re_s[0]
        # wstr1 = '\001'.join(
        #     [mid, name.encode('utf8'), wb['user']['screen_name'].encode('utf8'), str(t)])
        # wf_1.write(wstr1 + '\n')

        txt = wb['text'].replace(u'转发微博', '').replace(
            '\n', '')  # .split('//@')[0]
        wstr2 = '\001'.join([mid, name.encode('utf8'), wb['user'][
                            'screen_name'].encode('utf8'), str(t), txt.encode('utf8')])
        return wstr2

sc.textFile('/user/zlj/tmp/1').map(lambda x:fun(x)).saveAsTextFile('/user/zlj/tmp/1_parse')


# wf_2.close()
# wf_1.close()
