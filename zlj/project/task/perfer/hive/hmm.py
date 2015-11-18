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
    x0 = y[1]
    x1 = y[0]
    doc_id = x0[0]
    tf = x0[1]
    idf = x1
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


sql = '''
select
* from t_zlj_userbuy_item_hmm
limit 10000
'''

min_freq = 10
limit = 5
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("tfidftags", StringType(), True)
])

if __name__ == "__main__":
    rdd_pre = hiveContext.sql(sql).map(lambda x: (x.user_id, [i for i in x._c1.split('_') if len(i) > 0]))
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
    rddjoin=idfrdd.join(tfrdd)
    # rddjoin = tfrdd.join(idfrdd)
    # sorted(a,key=a[1],reverse=True)
    rst=rddjoin.map(lambda (x, y): join(x, y)).groupByKey().map(lambda (x, y): [x, "\t".join(
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
