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
user_id, concat_ws('_', collect_set(hmm)) as hmm

from
(
select item_id,hmm from t_zlj_item_hmm
where LENGTH (hmm)>3
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
import sys
if __name__ == "__main__":

    if len(sys.argv)<4:
        print ' py  min_freq limit feed_ds'
        sys.exit(0)
    min_freq=int(sys.argv[1])
    limit=int(sys.argv[2])
    feed_ds=sys.argv[3]
    # hmm_ds=sys.argv[4]
    rdd_pre = hiveContext.sql(sql_hmm%feed_ds).map(lambda x: (x.user_id, [i for i in x[1].split('_') if len(i) > 3]))
    # top_freq=5   and x[1]<top_freq
    words = set(rdd_pre.map(lambda x: x[1]).flatMap(lambda x: x).map(lambda x: (x, 1)).groupByKey().map(
        lambda (x, y): (x, len(y))).filter(lambda x: x[1] > min_freq).map(lambda x: x[0]).collect())
    broadcastVar = sc.broadcast(words)
    dict = broadcastVar.value
    rdd = rdd_pre.map(lambda (x, y): (x, [i for i in y if i in dict]))
    doc_num = rdd.count()
    # (word,(doc_id,tf))
    tfrdd = rdd.map(lambda (x, y): tf(x, y)).flatMap(lambda x: x)
    # word ,len
    dfrdd = rdd.map(lambda (x, y): df(x, y)).flatMap(lambda x: x).groupByKey().map(lambda (x, y): (x, len(y)))
    # word idf
    idfrdd = dfrdd.map(lambda (x, y): (x, math.log((doc_num + 1) * 1.0 / (y + 1))))
    # idfrdd.collectAsMap()
    # idfrdd.join()
    broadcastVar = sc.broadcast(idfrdd.collectAsMap())
    idfdict = broadcastVar.value
    rddjoin=idfrdd.join(tfrdd)
    joinrs=tfrdd.map(lambda  x: join1(x,idfdict))
    # rddjoin = tfrdd.join(idfrdd)
    # sorted(a,key=a[1],reverse=True)
    # rst=rddjoin.map(lambda (x, y): join(x, y))
    rst=joinrs.groupByKey().map(lambda (x, y): [x, "\t".join(
        [i[0] for index, i in enumerate(sorted(y, key=lambda t: t[-1], reverse=True)) if index < limit])])
    df=hiveContext.createDataFrame(rst,schema)
    hiveContext.registerDataFrameAsTable(df, 'tmptable')
    hiveContext.sql('drop table if EXISTS  t_zlj_userbuy_item_hmm_tfidf_tags')
    hiveContext.sql('create table t_zlj_userbuy_item_hmm_tfidf_tags as select * from tmptable')





    # rddjoin.map(lambda (x,y):join(x,y)).groupByKey().map(lambda  (x,y):(x,len(y)))
    # rdd.saveAsTextFile
    # rdd=hiveContext.sql(sql).map(lambda  x:[i for i in x._c1.split('_') if len(i)>0])
    #
    #
    #
    # from pyspark import SparkContext
    # from pyspark.mllib.feature import HashingTF
    # from pyspark.mllib.feature import IDF
    #
    # # sc = SparkContext()
    #
    # # Load documents (one per line).
    # documents = sc.textFile("...").map(lambda line: line.split(" "))
    #
    # hashingTF = HashingTF()
    # # tf = hashingTF.transform(documents)
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
