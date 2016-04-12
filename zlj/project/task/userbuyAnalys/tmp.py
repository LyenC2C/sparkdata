# coding:utf-8
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
from collections import Counter


f=open('D:\\workdata\\-commit-weibo_zlj_0406_time.txt')
import collections as coll
lv=[]

dic=coll.defaultdict(list)

for line in f:
    if len(line.split('\001'))<3:continue
    # print  line.split('\001')[2].decode('utf-8')
    w=line.split('\001')[-1]
    date=line.split('\001')[-4]
    if not date.isdigit():continue
    dic[date].append(w)




f=open("D:\\workdata\\NLP\\corpus\\BosonNLP_sentiment_score\\BosonNLP_sentiment_score.txt")
fwp=open("D:\\workdata\\NLP\\corpus\\BosonNLP_sentiment_score\\BosonNLP_sentiment_score_p",'w')
fwn=open("D:\\workdata\\NLP\\corpus\\BosonNLP_sentiment_score\\BosonNLP_sentiment_score_n",'w')
for line in f:
    if len(line.split())<2:continue
    word,s=line.split()
    if float(s)>1:fwp.write(word+'\n')
    if float(s)<-1:fwn.write(word+'\n')
# for k,v in dic.iteritems():
#     print k,len(v),sum([int(i) for  i in v])
# s= Counter('\t'.join(lv))
#
# for k,v in s.most_common(10):
#     print k,v