# encoding: utf-8


from pyspark import SparkContext,SparkConf
from pyspark.sql import *
from pyspark.sql.types import *
from itertools import groupby
import math

#
# sqlContext = SQLContext(sc)
# hiveContext = HiveContext(sc)
# hiveContext.sql('use wl_analysis')

def tf(x, y):
    num = len(y)
    lv = [(k, len(list(g)) * 1.0 / num) for k, g in groupby(sorted(y))]
    return [(i[0], (x, i[1])) for i in lv]


def df(x, y):
    lv = []
    for i in set(y):
        lv.append((i, 1))
    return lv


def tf_idf(x,dict):
    word=x[0]
    doc_id=x[1][0]
    tf=x[1][1]
    tfidf=tf*dict.get(word,0.5)
    return (doc_id,(word,tfidf))

# schema = StructType([
#     StructField("user_id", StringType(), True),
#     StructField("tfidftags", StringType(), True)
# ])

def groupvalue(y):
    s1={}
    for k,v in y:
        s1[k]=s1.get(k,0)+v
    lv=[(k,v) for k,v in s1.iteritems()]
    return lv


sql_hmm='''
select * from t_lt_base_record_item_segment_words_group

'''

import sys
if __name__ == "__main__":

    conf = SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.mb", "512")
    conf.set("spark.broadcast.compress", "true")
    conf.set("spark.driver.maxResultSize", "4g")
    conf.set("spark.akka.timeout", "300")
    conf.set("spark.shuffle.memoryFraction", "0.5")
    conf.set("spark.core.connection.ack.wait.timeout", "1800")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(appName="hmm", conf=conf)

    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)
    hiveContext.sql('use wl_analysis')

    if len(sys.argv)<2:
        print ' py  -min_freq limit'
        sys.exit(0)

    min_freq=int(sys.argv[1])  #限制最低词频
    limit=int(sys.argv[2])     #限制产出用户词云个数 30-50个
    rdd_pre = hiveContext.sql(sql_hmm).map(lambda x: (x.user_id, x[1].split('_')))
    # top_freq=5   and x[1]<top_freq

    # 统计所有的词
    words = set(rdd_pre.map(lambda x: x[1]).flatMap(lambda x: x).map(lambda x: (x, 1)).reduceByKey(lambda a,b:a+b)\
                .filter(lambda x: x[1] > min_freq).map(lambda x: x[0]).collect())
    broadcastVar = sc.broadcast(words)
    dict = broadcastVar.value

    # 过滤掉不需要的词
    rdd = rdd_pre.map(lambda (item_id, words): (item_id, [word for word in words if word in dict]))
    doc_num = rdd.count()

    # tf 值
    # (word,(doc_id,tf))
    tfrdd = rdd.map(lambda (item_id, words): tf(item_id, words)).flatMap(lambda x: x)
    # word ,count
    dfrdd = rdd.map(lambda (item_id, words): df(item_id, words)).flatMap(lambda x: x).reduceByKey(lambda a,b:a+b)
    # word idf
    idfrdd = dfrdd.map(lambda (word ,word_len): (word, math.log((doc_num + 1) * 1.0 / (word_len + 1))))

    #广播 idf
    broadcastVar_idf = sc.broadcast(idfrdd.collectAsMap())
    idfdict = broadcastVar.value

    #tfidf值
    joinrs = tfrdd.map(lambda x: tf_idf(x, idfdict))
    joinrs.map(lambda x: " ".join([x[0],x[1][0],str(x[1][1])])).saveAsTextFile('/user/lt/tfidf_words')

    #filter tfidf
    jrdd = joinrs.filter(lambda x: x[1][1] > 0.1).groupByKey()
    #(doc_id,[(word,tfidf),(word,tfidf)...])
    rd = jrdd.map(lambda (x, y): (x, groupvalue(y)))

    rst = rd.map(lambda (x, y): [x, "|".join(
        [i[0] + "_" + str(round(i[1], 4)) for index, i in
         enumerate(sorted(y, key=lambda t: t[-1], reverse=True)) if index < limit])])
    rst.map(lambda x: "\001".join(x)).saveAsTextFile('/user/lt/tfidf_doc')

