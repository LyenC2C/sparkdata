#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkConf

conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
conf.set("spark.kryoserializer.buffer.mb", "1024")
conf.set("spark.akka.frameSize", "100")
conf.set("spark.network.timeout", "1000s")
conf.set("spark.driver.maxResultSize", "8g")

sc = SparkContext(appName="user_cattags", conf=conf)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

from datetime import datetime
import rapidjson as json
import sys
from email  import utils
def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
        return res
    else: return res
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
        # time=[start_date.hour start_date.minute]
        if start_date.year<2015:continue
        t = int((end_date - start_date).total_seconds())
        name = wb['retweeted_status']['user']['screen_name']
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


sc.textFile('/commit/weibo_dc/weibo_repost_20161018.json').map(lambda x:try_fun(x)).filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/user/zlj/tmp/weibo_repost_20161018')
sc.textFile('/commit/weibo_dc/weibo_repost_20161019.json').map(lambda x:try_fun(x)).filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/user/zlj/tmp/weibo_repost_20161019')

sc.textFile('/commit/weibo_dc/weibo_repost_20161025.json').map(lambda x:try_fun(x)).filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/user/zlj/tmp/weibo_repost_20161025')
sc.textFile('/commit/weibo_dc/weibo_src_repost_20161026.json').map(lambda x:try_fun(x))\
    .filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/user/zlj/tmp/weibo_src_repost_20161026')

sc.textFile('/commit/weibo_dc/weibo_src_repost_20161027.json').map(lambda x:try_fun(x))\
    .filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/user/zlj/tmp/weibo_src_repost_20161027')

sc.textFile('/commit/weibo_dc/weibo_src_repost_20161028.json').map(lambda x:try_fun(x))\
    .filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/user/zlj/tmp/weibo_src_repost_20161028')

sc.textFile('/commit/weibo_dc/weibo_src_repost_20161029.json').map(lambda x:try_fun(x))\
    .filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/user/zlj/tmp/weibo_src_repost_20161029')


# scr weibo
def fun1(line):
    ob = json.loads(valid_jsontxt(line))
    if type(ob)!=type({}):return None
    res=()
    mid=ob['mid']
    text=ob['text']
    id=ob['user']['id']
    created_at=ob['created_at']
    return '\001'.join([str(i) for i in [mid,id,created_at,text]])

sc.textFile('/commit/weibo_dc/weibo_repost_20161027.json').map(lambda x:fun1(x)).filter(lambda x:x!=None).\
    saveAsTextFile('/user/zlj/tmp/weibo_src_20161027')

sc.textFile('/commit/weibo_dc/weibo_src_20161029.json').map(lambda x:fun1(x)).filter(lambda x:x!=None).\
    saveAsTextFile('/user/zlj/tmp/weibo_src_20161029')


# LOAD DATA   INPATH '/user/zlj/tmp/weibo_repost_20161019' OVERWRITE INTO TABLE t_zlj_dc_weibodata PARTITION (ds='20161019')
#
# LOAD DATA   INPATH '/user/zlj/tmp/weibo_repost_20161018' OVERWRITE INTO TABLE t_zlj_dc_weibodata PARTITION (ds='20161018')
#
# select * from t_zlj_dc_weibodata where ds='20161018' limit 10;