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

sc.textFile('/user/zlj/tmp/t_base_weibo_user_fri_tel').map(lambda x:len(x.split('\001')[-1].split(',')) ).histogram([1,1000,3000,5000,10000])
sc.textFile('/user/zlj/tmp/t_base_weibo_user_fri_tel').map(lambda x:x.split('\001')[-1].split(',') ).filter(lambda x:len(x)>10).take(10)


# weiboid 在手机号里。 关注列表可以不再手机号里

rdd_fr=sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_weibo_user_fri/ds=20160902').map(lambda x:(long(x.split('\001')[0]),x.split('\001')[1]))

rdd_tel=sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_uid_tmp/ds=wid').map(lambda x:(long(x.split('\001')[1]),0))


rdd_tel.filter(lambda x:x==u'1106909444').take(10)

br=sc.broadcast(rdd_tel.collect())
# rdd_fr.flatMap(lambda x:x).flatMap()
rdd=rdd_tel.join(rdd_fr).map(lambda (x,y):'\001'.join([str(x),list(y)[-1]])).saveAsTextFile('/user/zlj/tmp/t_base_weibo_user_fri_tel')




filter(lambda x:len(x.split('\001')[-1].split(','))>10000).count()
def f(x,dic):
    if x[0] not in dic:return None
    else :return  x

rdd_fr.map(lambda x:f(x,br.value)).filter(lambda x:x!=None).saveAsTextFile('/user/zlj/tmp/t_base_weibo_user_fri_tel')
rdd.saveAsTextFile('/user/zlj/tmp/t_base_weibo_user_fri_tel')