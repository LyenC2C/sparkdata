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

fw=open('D:\\workdata\\wine_brand_list_clean','w')

fwword=open('D:\\workdata\\wine_brand_list_dic','w')
for line in open('D:\\workdata\\wine_brand_list'):
    brands=[i.strip() for  i in line.split() ]
    for i in brands:
        fwword.write(i+'\n')
    if len(brands)<1:continue
    brand=brands[0].strip()
    leaf_brands=brands[1:]
    fw.write(brand+'\001'+'\t'.join(leaf_brands)+'\n')

