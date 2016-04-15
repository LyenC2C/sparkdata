#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
import time
import rapidjson as json

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.mllib.feature import IDFModel



sc = SparkContext()


path='/hive/warehouse/wlbase_dev.db/t_zlj_base_ec_item_title_cut/ds=20160216/part-00000'
# Load documents (one per line).
#  = sc.textFile(path).map(lambda line: line.split('\001')[-1].split('\t'))

doc=sc.textFile(path).map(lambda line: line.split('\001')).map(lambda x:(x[0],x[1].split()+[x[0]+'_doc']))

hashingTF = HashingTF(numFeatures=1<<22)
# [].extend([x[0] for i in xrange(7)])

hashingTF.indexOf()
ts= doc.mapValues(hashingTF.transform)
tf = hashingTF.transform(doc.map(lambda x:x[-1]).repartition(10))

index_doc=doc.map(lambda x:(x[0],hashingTF.indexOf(x[0]),[hashingTF.indexOf(i) for i in x[1]]))


idf1=IDFModel()
tf.cache()
idf = IDF(minDocFreq=1).fit(ts.map(lambda x:x[-1]).repartition(10))

ll=ts.mapValues(idf.transform)

# tsidf=ts.map(lambda x:(x[0],idf.transform(x[1])))

tfidf = idf.transform(ts.map(lambda x:x[-1]).repartition(10))
