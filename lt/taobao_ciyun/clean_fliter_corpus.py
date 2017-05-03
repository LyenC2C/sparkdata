#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

import  math
# sc = SparkContext(appName="3_clean_fliter_corpus")
# sqlContext = SQLContext(sc)
# hiveContext = HiveContext(sc)

from itertools import groupby

def tf(x, y):
    num = len(y)
    lv = [(k, len(list(g)) * 1.0 / num) for k, g in groupby(sorted(y))] #word tf
    return [(i[0], (x, i[1])) for i in lv] #word ,(userid)


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
        k=k.split('_')[0]
        s1[k]=s1.get(k,0)+v
    lv=[(k,v) for k,v in s1.iteritems()]
    return lv

def clean(x,word_set):
            lv=x.split()
            return " ".join([i for i in lv if i in  word_set ])
            # return " ".join(lv)
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

def index_weight(y):
    rs=[]
    lv=y.split('\003')
    for i in lv:
        kv=i.split()
        s=len(kv)
        for index,word in enumerate(kv,1):
            # word=v.split('_')[0]
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
        # joinrs.map(lambda x: " ".join([x[0],x[1][0],str(x[1][1])])).saveAsTextFile('/user/zlj/temp/1228data')
        jrdd=joinrs.filter(lambda x:x[1][1]>0.1).groupByKey()
        # jrdd.map(lambda (x,y):[i for i in y][0])
        rd=jrdd.map(lambda (x, y):(x,groupvalue(y)))
        rst=rd.map(lambda (x,y):[x, "|".join(
            [i[0].replace('_',"").replace('|',"")+"_"+str(round(i[1],4)) for index, i in enumerate(sorted(y, key=lambda t: t[-1], reverse=True)) if index < limit])])
        # rst.saveAsTextFil
        return rst

import sys
if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.mb","512")
    conf.set("spark.broadcast.compress","true")
    conf.set("spark.driver.maxResultSize","4g")
    conf.set("spark.akka.timeout", "300")
    conf.set("spark.shuffle.memoryFraction", "0.5")
    conf.set("spark.core.connection.ack.wait.timeout", "1800")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(appName="3_clean_fliter_corpus",conf=conf)

    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)
    hiveContext.sql('use wl_analysis')

    if len(sys.argv)<4:
        print ' py -usertfidf  min_freq limit feed_ds outputtable'
        print ' py -item min_freq limit feed_ds input_table input_docid, input_talbe_title  output_table'
        sys.exit(0)

    elif sys.argv[1]=='-cleandata':
        i=1
        top_freq=2000
        min_freq=sys.argv[i+1]
        limit=int(sys.argv[i+2])
        feed_ds=sys.argv[i+3]
        output_talbe=sys.argv[i+4]
        index_rdd=hiveContext.sql('select word,num from t_zlj_item_feed_title_cut_20151226_word_count ')
        count=index_rdd.count()
        # top_freq=count-top_freq

        word_set_rdd=index_rdd.map(lambda x:(x[0],x[1]))\
            .filter(lambda x:x[1]>int(min_freq)).sortBy(lambda x: x[1],ascending=False).zipWithIndex().filter(lambda x:x[1]>top_freq).map(lambda x:x[0][0])
        broadcastVal=sc.broadcast(word_set_rdd.collect())
        word_set=set(broadcastVal.value)
        corpus=hiveContext.sql('select user_id,title_cut from t_zlj_item_feed_title_cut_20151226')\
            .map(lambda x:x[0]+"\001"+clean(x[1],word_set)).filter(lambda x:x[0] is not None ).saveAsTextFile('/user/zlj/tmp/data/ds')
        hiveContext.sql('drop table if EXISTS  %s'%output_talbe)
        hiveContext.sql('create table %s like t_zlj_item_feed_title_cut_20151226'%output_talbe)
        hiveContext.sql("LOAD DATA  INPATH '/user/zlj/tmp/data/ds' OVERWRITE INTO TABLE %s "%output_talbe)
