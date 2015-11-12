# encoding: utf-8
__author__ = 'zlj'

from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)

hiveContext = HiveContext(sc)


