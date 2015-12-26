#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *

import  math
sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


def tf(x, y):
    num = len(y)
    lv = [(k, len(list(g)) * 1.0 / num) for k, g in groupby(sorted(y))]
    return [(i[0], (x, i[1])) for i in lv] #word ,id tf


def df(x, y):
    lv = []
    s=set(y)
    for i in s:
        lv.append((i, 1))
    return lv

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("tfidftags", StringType(), True)
])


def join1(x,dict):
    word=x[0]
    doc_id=x[1][0]
    tf=x[1][1]
    tfidf=tf*dict.get(word,0.5)
    if(word.endswith('-b')):
        tfidf=tfidf*1.3
        # word=word.replace('-b','')
    elif(word.endswith('-c')):
        tfidf=tfidf*1.2
        # word=word.replace('-c','')
    elif(word.endswith('_B1')):
        tfidf=tfidf*1.3
        word=word.replace('_B1','')
    elif(word.endswith('_B2')):
        tfidf=tfidf*1.2
        word=word.replace('_B2','')
    elif(word.endswith('_E1')):
        tfidf=tfidf*1.5
        word=word.replace('_E1','')
    elif(word.endswith('_E2')):
        tfidf=tfidf*1.3
        word=word.replace('_E2','')
    tfidf=tfidf*math.log(len(word)/2.0+2,2)
    return (doc_id,(word,tfidf))

def groupvalue(y):
    s1={}
    for k,v in y:
        s1[k]=s1.get(k,0)+v

    lv=[(k,v) for k,v in s1.iteritems()]
def clean(x,word_set):
            lv=x.split()
            return " ".join([i for i in lv if i in  word_set ])
def join(y):
    rs=[]
    for i in y:
        kv=i.split()
        s=len(kv)
        for index,v in enumerate(kv,1):
            if  not v.find('_n'):continue
            word=v.split('_')[0]
            if  len(word)<2:continue
            if word.replace('.','',1).isdigit(): continue
            if index==(s-3):
                rs.append(word+'_E2')
            elif index==(s-2):
                rs.append(word+'_E1')
            elif index==1:
                rs.append(word+'_B1')
            else:
                rs.append(word)
    return rs


def tfidf(corpus,limit):
        doc_num=50000000
        tfrdd = corpus.map(lambda (x, y): tf(x, y)).flatMap(lambda x: x)
        dfrdd = corpus.map(lambda (x, y): df(x, y)).flatMap(lambda x: x).reduceByKey(lambda a,b:a+b)
        # word idf
        idfrdd = dfrdd.map(lambda (x, y): (x, math.log((doc_num + 1) * 1.0 / (y + 1))))
        broadcastVar = sc.broadcast(idfrdd.collectAsMap())
        idfdict = broadcastVar.value
        joinrs=tfrdd.map(lambda  x: join1(x,idfdict))
        jrdd=joinrs.groupByKey()
        rst=jrdd.map(lambda (x, y):(x,groupvalue(y))).map(lambda (x,y):[x, "\t".join(
            [i[0].replace('_',"")+"_"+str(round(i[1],4)) for index, i in enumerate(sorted(y, key=lambda t: t[-1], reverse=True)) if index < limit])])
        return rst

import sys
if __name__ == "__main__":
    hiveContext.sql('use wlbase_dev')
    if len(sys.argv)<4:
        print ' py -usertfidf  min_freq limit feed_ds outputtable'
        print ' py -item min_freq limit feed_ds input_table input_docid, input_talbe_title  output_table'
        sys.exit(0)
    elif sys.argv[1]=='-new':
        i=1
        top_freq=2000
        min_freq=sys.argv[1]
        limit=int(sys.argv[i+2])
        feed_ds=sys.argv[i+3]
        output_talbe=sys.argv[i+4]
        index_rdd=hiveContext.sql('select word,num from t_zlj_item_feed_title_cut_20151226_word_count')
        count=index_rdd.count()
        top_freq=count-top_freq

        word_set_rdd=index_rdd.map(lambda x:(x[0],x[1]))\
            .filter(lambda x:x[1]>1).sortBy(lambda x: x[1],ascending=False).zipWithIndex().filter(lambda x:x[1]<top_freq).map(lambda x:x[0][0])
        broadcastVal=sc.broadcast(word_set_rdd.collect())
        word_set=broadcastVal.value
        corpus=hiveContext.sql('select user_id,title_cut from t_zlj_item_feed_title_cut_20151226')\
            .map(lambda x:(x[0],clean(x[1]))).filter(lambda x:x[0] is not None ).groupByKey().map(lambda (x,y):(x,join(y)))
        rst=tfidf(corpus,limit)
        df=hiveContext.createDataFrame(rst,schema)
        hiveContext.registerDataFrameAsTable(df, 'tmptable')
        hiveContext.sql('drop table if EXISTS  %s'%output_talbe)
        hiveContext.sql('create table %s as select * from tmptable'%output_talbe)