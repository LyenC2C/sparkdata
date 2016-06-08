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
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    return str(res).replace('\n',"").replace("\r","")

def parse(x):
    ob=json.loads(valid_jsontxt(x))
    if type(ob)==type(1.0):return  None
    title=ob.get('title','-')
    sold=ob.get('sold','-')
    reservePrice=ob.get('reservePrice','-')
    maxpage=ob.get('maxpage','-')
    rate=ob.get('rate','-')
    picUrl=ob.get('picUrl','-')
    item_id=ob.get('item_id','-')
    if len(item_id)<2: return None
    page=ob.get('page','-')
    salePrice=ob.get('salePrice','-')
    return [ str(i) for  i in [title,sold,reservePrice,maxpage,rate,picUrl,item_id,page,salePrice]]


sc.textFile('/commit/taobao/shopitemhtml/20160524/').map(lambda x: parse(x)).filter(lambda x:x is not None)\
    .map(lambda x:'\001'.join(x)).saveAsTextFile('/commit/taobao/shopitemhtml/20160524_hivedata')