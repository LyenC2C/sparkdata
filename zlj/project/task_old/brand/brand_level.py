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



def wine_levle(logprice):
    level=-1
    if logprice<=3:
        level=1
    if logprice==4:
        level=2
    if logprice==5:
        level=3
    if logprice==6:
        level=4
    if logprice>6:
        level=5
    return level