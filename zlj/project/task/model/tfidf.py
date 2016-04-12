#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql.types import *
import time
import rapidjson as json
import math
from pyspark.mllib.feature import HashingTF


from itertools import groupby

conf=SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
sc = SparkContext(appName="tfidf",conf=conf)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

# def groupvalue(y):
#
#     lv=[(k.split('-')[0],v) for k,v in s1.iteritems()]

def join(x, y):
    word = x
    tfinfo = y[1]
    idfinfo = y[0]
    doc_id = tfinfo[0]
    tf = tfinfo[1]
    idf = idfinfo
    return (doc_id, (word, tf * idf))


def tf(x, y):
    num = len(y)
    lv = [(k, len(list(g)) * 1.0 / num) for k, g in groupby(sorted(y))]
    return [(i[0], (x, i[1])) for i in lv]


def df(x, y):
    lv = []
    s=set(y)
    for i in s:
        lv.append((i, 1))
    return lv

def join1(x,dict):
    word=x[0]
    doc_id=x[1][0]
    tf=x[1][1]
    tfidf=tf*dict.get(word,0.5)
    return (doc_id,(word,tfidf))
'''
rdd=[(k,[word word word])]
'''
def clean(rdd,top_freq,min_freq):
    filter_wordset=rdd.map(lambda (k,v_list):v_list).flatMap(lambda x:x).map(lambda x:(x,1))\
        .reduceByKey(lambda a,b:a+b).filter(lambda (x,y): y>min_freq and y<top_freq).map(lambda (x,y):x).collect()
    broadcastVar = sc.broadcast(filter_wordset)
    worddic = broadcastVar.value
    rdd_clean=rdd.map(lambda (k,v_list):(k,[i for i in v_list if i in worddic]))
    return rdd_clean



def tfidf(rdd_clean,limit):
    doc_num = 50000000
    # (word,(doc_id,tf))
    tfrdd = rdd_clean.map(lambda (x, y): tf(x, y)).flatMap(lambda x: x)
    # word ,len
    # dfrdd = rdd.map(lambda (x, y): df(x, y)).flatMap(lambda x: x).groupByKey().map(lambda (x, y): (x, len(y)))
    dfrdd = rdd_clean.map(lambda (x, y): df(x, y)).flatMap(lambda x: x).reduceByKey(lambda a,b:a+b)
    # word idf
    idfrdd = dfrdd.map(lambda (x, y): (x, math.log((doc_num + 1) * 1.0 / (y + 1))))
    # idfrdd.collectAsMap()
    # idfrdd.join()
    broadcastVar = sc.broadcast(idfrdd.collectAsMap())
    idfdict = broadcastVar.value
    # rddjoin=idfrdd.join(tfrdd)
    # worddicRs = dict(zip(worddic.values(),worddic.keys()))
    joinrs=tfrdd.map(lambda  x: join1(x,idfdict))
    # rddjoin = tfrdd.join(idfrdd)
    # sorted(a,key=a[1],reverse=True)
    # rst=rddjoin.map(lambda (x, y): join(x, y))
    jrdd=joinrs.groupByKey()
    # jrdd.map(lambda (x,y):str(x)+'\001'.join([str(k)+":"+str( v) for k,v in y ])).saveAsTextFile('/user/zlj/project/termweight/jointfidf_rs')
    rst=jrdd.map(lambda (x, y):(x,list(y))).map(lambda (x,y):[x,
        [i[0]+"_"+str(round(i[1],4)) for index, i in enumerate(sorted(y, key=lambda t: t[-1], reverse=True)) if index < limit]])
    return rst


import sys
if __name__ == "__main__":
    input=sys.argv[1]
    output=sys.argv[2]
    min_freq=int(sys.argv[3])
    limit=int(sys.argv[4])
    top_freq=1000000
    if len(sys.argv)==6:
        top_freq=int(sys.argv[5])
    # rdd=sc.textFile(input).map(lambda x:x.split()).map(lambda  x:(x[0],[i.split('_')[0] for i in x[-1].split('\001') if len(i)>0]))
    rdd=sc.textFile(input).map(lambda x:x.split('\001')).map(lambda  x:(x[0],[i for i in x[-1].split('\t') if len(i)>0]))
    rdd_clean=clean(rdd,top_freq,min_freq)
    # rdd_clean.saveAsTextFile(output)
    rdd_tfidf=tfidf(rdd_clean,limit)
    rdd_tfidf.map(lambda (x,y):x+'\001'+ "\t".join(y)).saveAsTextFile(output)





#


sc.textFile('/user/zlj/tid_base_loc.dir/part-00000').map(lambda x:len(x.split('\001'))).distinct()