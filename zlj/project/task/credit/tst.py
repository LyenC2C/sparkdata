#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
import time
import rapidjson as json

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content

def f(x):
    ob=json.loads(valid_jsontxt(x))
    try:
         uid=ob.get('uid','')
    except:return None
    alipay=ob.get('alipay','-')
    buycnt=ob.get('buycnt','-')
    verify=ob.get('verify','-')
    regtime=ob.get('regtime','-')
    nick=ob.get('nick','-')
    location=ob.get('location','-')
    return (uid,[uid,alipay,buycnt,verify,regtime,nick,location])

rdd=sc.textFile('/commit/taobao/userinfo/tbuid*/*').map(lambda x:f(x)).filter(lambda x:x  is not None).groupByKey()\
    .map(lambda (x,y):[str(i) for i in list(y)[0]]).map(lambda x:'\001'.join(x))

rdd.saveAsTextFile('/commit/taobao/userinfo/data')