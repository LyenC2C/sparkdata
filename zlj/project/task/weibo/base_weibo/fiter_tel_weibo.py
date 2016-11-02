#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkConf
import rapidjson as json
conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
conf.set("spark.kryoserializer.buffer.mb", "1024")
conf.set("spark.akka.frameSize", "100")
conf.set("spark.network.timeout", "1000s")
conf.set("spark.driver.maxResultSize", "8g")

sc = SparkContext(appName="user_cattags", conf=conf)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
# 找出真实手机号关联的数据
sc.textFile('/user/zlj/tmp/t_base_weibo_user_fri_tel').map(lambda x:len(x.split('\001')[-1].split(',')) ).histogram([1,1000,3000,5000,10000])
sc.textFile('/user/zlj/tmp/t_base_weibo_user_fri_tel').map(lambda x:x.split('\001')[-1].split(',') ).filter(lambda x:len(x)>10).take(10)


rdd_fr=sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_weibo_user_fri/ds=20160902').map(lambda x:(long(x.split('\001')[0]),x.split('\001')[1]))

rdd_tel=sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_uid_tmp/ds=wid').map(lambda x:(long(x.split('\001')[1]),0))
rdd_tel.meanApprox()

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

sc.textFile('/user/zlj/tmp/t_base_weibo_user_fri_tel/').map(lambda x:x.split('\001')[0]).distinct().count()
def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
        return res
    else: return res
def f_tel_tb(x):
    ob=json.loads(valid_jsontxt(x))
    if type(ob)!=type({}):return None
    if ob==None:return None
    if not ob.get('tel','') or  not ob.get('TB',''):
        return None
    else :return  [ob['tel'],ob['TB']]


def f_info(x):
    ob=json.loads(valid_jsontxt(x))
    if type(ob)!=type({}):return None
    if ob==None:return None
    else :return  [
        ob.get('QQ','-'),
        ob.get('tel','-'),
        ob.get('real_name','-'),
        ob.get('snwb','-'),
        ob.get('QQWB','-'),
        ob.get('TB','-'),
        ob.get('email','-')]

rdd=sc.textFile('/user/zlj/refer/refer.v0906.dir/').\
    map(lambda x:f_info(x)).filter(lambda x:x!=None).map(lambda x: '\001'.join([str(i) for i in x])).repartition(20).saveAsTextFile('/user/zlj/refer/info')

rdd.map(lambda x:x[0]).distinct()
sc.textFile().distinct()



def f_qq_tb(x):
    ob=json.loads(valid_jsontxt(x))
    if type(ob)!=type({}):return None
    if ob==None:return None
    if not ob.get('QQ','') or  not ob.get('TB',''):
        return None
    else :return '\001'.join([ob['QQ'],ob['TB']])

sc.textFile('/user/zlj/refer/refer.v0906.dir/').\
    map(lambda x:f_qq_tb(x)).filter(lambda x:x!=None).repartition(20).saveAsTextFile('/user/zlj/refer/qq_tb')


sc.textFile('/user/zlj/tmp/t_base_weibo_user_fri_tel').repartition(200).saveAsTextFile('/user/zlj/tmp/t_base_weibo_user_fri_tel200')




sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_weibo_user_fri/ds=20160902').map(lambda x:x.split('\001')).flatMap(lambda x:x).\
    map(lambda x:x.split(',')).flatMap(lambda x:x).distinct().saveAsTextFile('/user/zlj/tmp/weiboid_distinct')

import  rapidjson as json
def f(x):
    try:
        ob=json.loads(valid_jsontxt(x))
        if type(ob)!=type({}):return None
        return (str(ob.get('Portrait_tag','')),str(ob.get('username','')))
    except: return None


def f(x):
    ob=json.loads(valid_jsontxt(x))
    if type(ob)!=type({}):return None
    return (str(ob.get('Portrait_tag','')),str(ob.get('username','')))


rdd1=sc.textFile('/commit/credit/baidu/160914.name.tag.json').map(lambda x:f(x)).filter(lambda x:x !=None )
rdd2=sc.textFile('/commit/credit/baidu/160914.tel.tag.json').map(lambda x:f(x)).filter(lambda x:x !=None and  type(x[0])==type(''))

rdd1.map(lambda x:'\001'.join(x)).saveAsTextFile('/user/zlj/tmp/160914.name.tag.json_txt')
rdd2.map(lambda x:'\001'.join(x)).saveAsTextFile('/user/zlj/tmp/160914.tel.tag.json_txt')
data=rdd2.join(rdd1).cache()
data.map(lambda (x,y):[list(y)[0],list(y)[1],x]).map(lambda x:'\001'.join(x)).saveAsTextFile('/user/zlj/tmp/tieba_tel_name_tag')


