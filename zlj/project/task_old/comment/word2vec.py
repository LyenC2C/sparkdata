#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

'''
word2vec 语料准备 。tag 放入预料中
'''

# rdd=sc.textFile('/user/zlj/data/feed_2015_alicut_parse/parse_split_clean_cut_part-00000_0002')
rdd=sc.textFile('/user/zlj/data/feed_2015_alicut_parse/')

def cut(x):
    lv=x.split()
    rs=[]
    if len(lv)!=5: return None
    else:
      item_id,feed_id,user_id,feed,impr=lv
      for i in impr.split('|'):
          if ":" in i:
              ls=i.strip().split(',')
              if len(ls)!=4: continue
              word,degree,neg,impr=i.split(',')
              words=word.split('\002')
              length=len(words)
              rs.extend(words[0:length/2])
              rs.append(impr)
              rs.extend(words[length/2:length])
    return ' '.join(rs)
rdd.map(lambda x:cut(x)).filter(lambda x:x is not None and len(x)>0).saveAsTextFile('/user/zlj/data/feed_2015_alicut_parse_w2v_corpus')