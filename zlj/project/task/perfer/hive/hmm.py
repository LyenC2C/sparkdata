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
    min_freq=int(sys.argv[1])  #限制最低词频
    limit=int(sys.argv[2])     #限制产出用户词云个数 30-50个
    feed_ds=sys.argv[3]
    # hmm_ds=sys.argv[4]
    rdd_pre = hiveContext.sql(sql_hmm%feed_ds).map(lambda x: (x.user_id, [i for i in x[1].split('_') if len(i) > 3]))
    # top_freq=5   and x[1]<top_freq
    # 统计所有的词
    words = set(rdd_pre.map(lambda x: x[1]).flatMap(lambda x: x).map(lambda x: (x, 1)).groupByKey().map(
        lambda (x, y): (x, len(y))).filter(lambda x: x[1] > min_freq).map(lambda x: x[0]).collect())
    broadcastVar = sc.broadcast(words)
    dict = broadcastVar.value

    # 过滤掉不需要的词
    rdd = rdd_pre.map(lambda (item_id, words): (item_id, [word for word in words if word in dict]))
    doc_num = rdd.count()
    # tf 值
    # (word,(doc_id,tf))
    tfrdd = rdd.map(lambda (item_id, words): tf(item_id, words)).flatMap(lambda x: x)
    # word ,len
    dfrdd = rdd.map(lambda (item_id, words): df(item_id, words)).flatMap(lambda x: x).groupByKey().map(lambda (x, y): (x, len(y)))
    # word idf
    idfrdd = dfrdd.map(lambda (word ,word_len): (word, math.log((doc_num + 1) * 1.0 / (word_len + 1))))

    #广播 idf


