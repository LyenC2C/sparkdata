#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkConf
# import rapidjson as json
conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
conf.set("spark.kryoserializer.buffer.mb", "1024")
conf.set("spark.akka.frameSize", "100")
conf.set("spark.network.timeout", "1000s")
conf.set("spark.driver.maxResultSize", "8g")

sc = SparkContext(appName="user_cattags", conf=conf)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
from pyspark.mllib.util import MLUtils
from pyspark.mllib.stat import Statistics


'''
  35049_4:18.9 35049_5:18.9 35049_6:0.0 35034_1:16.9 35034_2:1
返回: 特征名字
'''
def get_feature_name(line):
    return set([ i.split(':')[0] for i in line.split()])

'''
35049_1:18.9 35049_2:1 35049_3:18.9
return:1:18.9 2:1 3:18.9
'''
def get_feature_index(line ,dic):
    ls=[(dic[i.split(':')[0]],i.split(':')[1] )for i in line.split()]
    sort_data=sorted(ls,key=lambda x:x[0])
    return ' '.join([str(k)+':'+str(v) for k,v in sort_data])


# hiveContext.sql('use wlbase_dev')

rdd= hiveContext.sql('select * from wlservice.t_zlj_tmp_rong360_1w_record_level3_feature ')

featurs=rdd[['_c1']].map(lambda x:get_feature_name(x._c1)).flatMap(lambda x:x).distinct().collect()
featur_index={v:str(index) for index,v in enumerate(featurs)}
featur_index_value = sc.broadcast(featur_index).value

rdd.map(lambda x:x.tel+' '+get_feature_index(x._c1,featur_index_value)).saveAsTextFile('/user/zlj/tmp/featue')

# rdd['f']rdd[['_c1']].map(lambda x:MLUtils._parse_libsvm_line('1 '+get_feature_index(x._c1,featur_index_value)))
#
# rdd.withColumn('f_spare',MLUtils._parse_libsvm_line('1 '+get_feature_index(rdd._c1,featur_index_value)))

# MLUtils._parse_libsvm_line()