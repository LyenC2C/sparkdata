#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

# from pyspark import SparkContext
# from pyspark.sql import *
# from pyspark.sql.types import *
# import time
# import rapidjson as json
#
# sc = SparkContext(appName="cmt")
# sqlContext = SQLContext(sc)
# hiveContext = HiveContext(sc)

from tgrocery import Grocery

grocery = Grocery('sample')

train_src = [
('education', 'Student debt to cost Britain billions within decades'),
('education', 'Chinese education for TV experiment'),
('sports', 'Middle East and Asia boost investment in top level sports'),
('sports', 'Summit Series look launches HBO Canada sports doc series: Mudhar')
]

grocery.train('/home/hadoop/tmp/tgrocery/train_file.txt')

print grocery.predict("7款清爽眼部卸妆液 卸掉残妆不留暗沉")
