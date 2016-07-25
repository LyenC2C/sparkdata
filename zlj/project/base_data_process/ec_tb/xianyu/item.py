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
conf.set("spark.kryoserializer.buffer.mb", "1024")
conf.set("spark.akka.frameSize", "100")
conf.set("spark.network.timeout", "1000s")
conf.set("spark.driver.maxResultSize", "8g")

sc = SparkContext(appName="xianyu_item", conf=conf)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


# def parse_xianyu_item(line):
#     ob=json.loads(line)
#     vip_level=ob.get('vip-level','-')
#     price=ob.get('price','-')
#     price_unit=ob.get('price-unit','-')
#     comments_num=ob.get('comments-num','-')
#     category=ob.get('category','-')
#     grab_time=ob.get('grab-time','-')
#     title=ob.get('title','-')
#     seller=ob.get('seller','-')
#     item_url=ob.get('item-url','-')
#     location=ob.get('location','-')
enumerate

def  parse(line):
    if '"ret":400' in line :return None

    ob=json.loads(line[7:-3])
    idleItemSearch_data=ob.get['idleItemSearch@2']['data']
    totalCount=int(idleItemSearch_data.get('totalCount','0'))
    if totalCount>0 :
        ''
    userPersonalInfo_data=ob['userPersonalInfo@2']['data']
    userId=userPersonalInfo_data['userId']
    userNick=userPersonalInfo_data['userNick']
    birthday=userPersonalInfo_data.get('birthday','' )
    city=userPersonalInfo_data['city']
    constellation=userPersonalInfo_data['constellation']
    gender=userPersonalInfo_data['gender']

    print userId, birthday, constellation,gender,totalCount

for line in open('')
    parse(line)