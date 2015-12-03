# encoding: utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

''.startswith()
root='/home/hadoop/common/segfile/tlpdata'
from pyltp import Segmentor
segmentor = Segmentor()
segmentor.load(root+'cws.model')


sc.textFile('/user/zlj/temp/100line_title').map(lambda x: ' '.join(segmentor.segment(x))).saveAsTextFile('/user/zlj/temp/100line_title_ltp')