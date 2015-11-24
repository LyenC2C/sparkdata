# encoding: utf-8
__author__ = 'zlj'

from pyspark import SparkContext

from pyspark.sql import *
from pyspark.sql.types import *
from itertools import groupby

import math

sc = SparkContext(appName="hmm")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

hiveContext.sql('use wlbase_dev')

'''

'''
def join(x, y):
    word = x
    tfinfo = y[1]
    idfinfo = y[0]
    doc_id = tfinfo[0]
    tf = tfinfo[1]
    idf = idfinfo
    return (doc_id, (word, tf * idf))


# ['apple', 'banana', 'apple', 'strawberry', 'banana', 'lemon']
# [('apple', 2), ('banana', 2), ('lemon', 1), ('strawberry', 1)]
def tf(x, y):
    num = len(y)
    lv = [(k, len(list(g)) * 1.0 / num) for k, g in groupby(sorted(y))]
    return [(i[0], (x, i[1])) for i in lv]


def df(x, y):
    lv = []
    for i in set(y):
        lv.append((i, 1))
    return lv


# sql = '''
# select
# * from t_zlj_userbuy_item_hmm
# '''

# min_freq = 100
# limit = 5
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("tfidftags", StringType(), True)
])

def join1(x,dict):
    word=x[0]
    doc_id=x[1][0]
    tf=x[1][1]
    tfidf=tf*dict.get(word,0.5)
    return (doc_id,(word,tfidf))

sql_hmm='''
select
/*+ mapjoin(t1)*/
user_id, concat_ws(' ', collect_set(hmm)) as hmm

from
(
select item_id,title_cut as hmm from t_zlj_corpus_item_seg
where LENGTH (title_cut)>3
)t1
join
(
select item_id,user_id from t_base_ec_item_feed_dev

where ds>%s
and  LENGTH (user_id)>0

)t2
on t1.item_id=t2.item_id
group by user_id
'''


sql_tfidf='''
select
/*+ mapjoin(t1)*/
user_id, concat_ws(' ', collect_set(hmm)) as hmm

from
(
select user_id as item_id,tfidftags  as hmm from t_zlj_corpus_item_seg_tfidf
)t1
join
(
select item_id,user_id from t_base_ec_item_feed_dev

where ds>%s
and  LENGTH (user_id)>0

)t2
on t1.item_id=t2.item_id
group by user_id
'''


# sql_count='''
# select
# from
# (
# select item_id,user_id from t_base_ec_item_feed_dev
#
# where ds>%s
# and  LENGTH (user_id)>0
# group by user_id
# )t
# '''


def hashid(freq,hashNum,word):
    i = hash(word)%hashNum
    freq[i] = freq.get(i, 0) + 1.0

'''
rdd  {k,[w1 w2 w3 .....]}

return rdd [id, word_tfifd word_tfidf]
'''

def tcount(lv):
    s=set(lv)
    re=[]
    for i in s:
        re.append((i,lv.count(i)))
    return re

def tfidf(rdd_pre,top_freq,min_freq,limit):
    # words = set(rdd_pre.map(lambda x: x[1]).flatMap(lambda x: x).map(lambda x: (x, 1)).reduceByKey(lambda a,b:a+b).filter(lambda x: x[1] > min_freq).map(lambda x: x[0]).collect())
    words = set(rdd_pre.map(lambda x: tcount(x[1])).flatMap(lambda x: x).coalesce(100).reduceByKey(lambda a,b:a+b).filter(lambda x: x[1] > min_freq).map(lambda x: x[0]).collect())

    broadcastVar = sc.broadcast(words)
    dict = broadcastVar.value
    # doc_num = rdd_pre.map(lambda x:1).count()
    doc_num = 50000000
    rdd = rdd_pre.map(lambda (x, y): (x, [i for i in y if i in dict]))

    # (word,(doc_id,tf))
    tfrdd = rdd.map(lambda (x, y): tf(x, y)).flatMap(lambda x: x)
    # word ,len
    # dfrdd = rdd.map(lambda (x, y): df(x, y)).flatMap(lambda x: x).groupByKey().map(lambda (x, y): (x, len(y)))
    dfrdd = rdd.map(lambda (x, y): df(x, y)).flatMap(lambda x: x).reduceByKey(lambda a,b:a+b)
    # word idf
    idfrdd = dfrdd.map(lambda (x, y): (x, math.log((doc_num + 1) * 1.0 / (y + 1))))
    # idfrdd.collectAsMap()
    # idfrdd.join()
    broadcastVar = sc.broadcast(idfrdd.collectAsMap())
    idfdict = broadcastVar.value
    # rddjoin=idfrdd.join(tfrdd)
    joinrs=tfrdd.map(lambda  x: join1(x,idfdict))
    # rddjoin = tfrdd.join(idfrdd)
    # sorted(a,key=a[1],reverse=True)
    # rst=rddjoin.map(lambda (x, y): join(x, y))
    rst=joinrs.groupByKey().map(lambda (x, y): [x, "\t".join(
        [i[0]+"_"+str(i[1]) for index, i in enumerate(sorted(y, key=lambda t: t[-1], reverse=True)) if index < limit])])
    return rst
'''
spark-submit  --total-executor-cores  200   --executor-memory  20g  --driver-memory 20g  tfidf.py -item 200   5  20143 t_zlj_corpus_item_seg item_id title_cut  t_zlj_corpus_item_seg_tfidf
'''
import sys
if __name__ == "__main__":

    if len(sys.argv)<4:
        print ' py -usertfidf  min_freq limit feed_ds outputtable'
        print ' py -item min_freq limit feed_ds input_table input_docid, input_talbe_title  output_table'
        sys.exit(0)
    if sys.argv[1]=='-usertfidf':
        i=1
        min_freq=int(sys.argv[i+1])
        limit=int(sys.argv[i+2])
        feed_ds=sys.argv[i+3]
        output_talbe=sys.argv[i+4]
        rdd_pre = hiveContext.sql(sql_tfidf%feed_ds).map(lambda x: (x.user_id, [i.split('_')[0] for i in x[1].split()]))
        rst=tfidf(rdd_pre,top_freq=1000,min_freq=min_freq,limit=limit)
        df=hiveContext.createDataFrame(rst,schema)
        hiveContext.registerDataFrameAsTable(df, 'tmptable')
        # hiveContext.sql('drop table if EXISTS  t_zlj_userbuy_item_tfidf_tags')
        # hiveContext.sql('create table t_zlj_userbuy_item_tfidf_tags as select * from tmptable')
        hiveContext.sql('drop table if EXISTS  %s'%output_talbe)
        hiveContext.sql('create table %s as select * from tmptable'%output_talbe)

    elif sys.argv[1]=='-item':
        i=1
        min_freq=int(sys.argv[i+1])
        limit=int(sys.argv[i+2])
        feed_ds=sys.argv[i+3]
        input_table=sys.argv[i+4]
        input_table_docid=sys.argv[i+5]
        input_table_title=sys.argv[i+6]
        output_talbe=sys.argv[i+7]

        sql_itemtitle='''
        select
         %s,
         %s
        from
        %s
        '''
        # hmm_ds=sys.argv[4]
        rdd_pre = hiveContext.sql(sql_itemtitle%(input_table_docid,input_table_title,input_table)).map(lambda x: (x[0], [i for i in x[1].split() if len(i) > 1]))
        rst=tfidf(rdd_pre,top_freq=1000,min_freq=100,limit=5)
        df=hiveContext.createDataFrame(rst,schema)
        hiveContext.registerDataFrameAsTable(df, 'tmptable')
        hiveContext.sql('drop table if EXISTS  %s'%output_talbe)
        hiveContext.sql('create table %s as select * from tmptable'%output_talbe)
    elif sys.argv[1]=='-corpus':
        i=1
        min_freq=int(sys.argv[i+1])
        limit=int(sys.argv[i+2])
        feed_ds=sys.argv[i+3]
        output_talbe=sys.argv[i+4]
        rdd_pre = hiveContext.sql(sql_tfidf%feed_ds).map(lambda x: (x.user_id, [i.split('_')[0] for i in x[1].split()]))
        words = set(rdd_pre.map(lambda x: tcount(x[1])).flatMap(lambda x: x).coalesce(100).reduceByKey(lambda a,b:a+b).filter(lambda x: x[1] > min_freq).map(lambda x: x[0]).collect())
        # wordmap={}
        # for  index,value in enumerate(words,1):
        #     wordmap[value]=index
        broadcastVar = sc.broadcast(words)
        dict = broadcastVar.value
        rdd = rdd_pre.map(lambda (x, y): (x, [i for i in y if i in dict])).map(lambda (x,y):x+"\t"+" ".join([ i[0]+":"+str(i[1]) for i in tcount(y)]))
        rdd.saveAsTextFile("/user/zlj/temp/user_corpus")




    # rddjoin.map(lambda (x,y):join(x,y)).groupByKey().map(lambda  (x,y):(x,len(y)))
    # rdd.saveAsTextFile
    # rdd=hiveContext.sql(sql).map(lambda  x:[i for i in x._c1.split('_') if len(i)>0])
    #
    #
    #
    # from pyspark import SparkContext
    # from pyspark.mllib.feature import IDF
    #
    # # sc = SparkContext()
    #
    # # Load documents (one per line).
    # documents = sc.textFile("...").map(lambda line: line.split(" "))
    #

    # hashingTF = HashingTF()
    # tf = hashingTF.transform(documents)
    # tfrdd=rdd.map(lambda  x: (x[0],hashingTF.transform(x[1])))
    #
    # # tfrdd = hashingTF.transform(rdd)
    # tfrdd.cache()
    # idf=IDF().fit(tfrdd.values())
    # fff=idf.transform
    # tfidf=tfrdd.map(lambda  x: (x[0],fff(x[1])))
    #
    # tmp=[]
    # for x,y in tfrdd.collect():
    #     tmp.append((x,idf.transform(y)))
    #
    # rs=sc.parallelize(tmp)
    #
    # # tfidf = idf.transform(tfrdd)
    # # val num_idf_pairs = tf_num_pairs.mapValues(v => idf.transform(v))
