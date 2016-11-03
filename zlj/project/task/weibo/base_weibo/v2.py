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

def try_fun(x):
    ob=json.loads(x)
    if type(ob)!=type({}):return  None
    try:
        if type(ob['ids'])!=type([]):return None
        return [str(ob['uid']),','.join(set([str(i) for i in ob['ids']]))]
    except:return None

sc.textFile('/commit/weibo/friendships/20161101/').map(lambda x:try_fun(x)).\
    filter(lambda x:x is not None  and x[0] is not None and x[1] is not None)\
    .map(lambda x:'\001'.join(x)).saveAsTextFile('/user/zlj/tmp/weibo_friendships_20161101')


# 555890126
sc.textFile('/commit/weibo/userinfo/20161101').count()

869686646
sc.textFile('/commit/weibo/uid_8.8y_20161102').count()

762848995
sc.textFile('/commit/weibo/uid_8.8y_20161102').distinct().count()

# 重复数 165691458