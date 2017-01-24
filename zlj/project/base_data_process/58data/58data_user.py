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

sc = SparkContext(appName="weibo_user", conf=conf)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
        return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    else: return res

def parse(line):
    ob=json.loads(valid_jsontxt(line))
    if type(ob)!=type({}):return None
    t_action = ob.get('action','-'),
    cateid = ob.get('cateid','-'),
    catename = ob.get('catename','-'),
    decrypted_tel = ob.get('decrypted_tel','-'),
    infoid = ob.get('infoid','-'),
    # infoimg = ob.get('infoimg','-'),
    # invitation = ob.get('invitation','-'),
    isbiz = ob.get('isbiz','-'),
    nickname = ob.get('nickname','-'),
    online = ob.get('online','-'),
    rootcateid = ob.get('rootcateid','-'),
    title = ob.get('title','-'),
    tradeline = ob.get('tradeline','-'),
    uid = ob.get('uid','-'),
    uname = ob.get('uname' ,'-'),
    rs=[t_action ,
            cateid   ,
            catename ,
            decrypted_tel,
            infoid   ,
            isbiz,
            nickname ,
            online   ,
            rootcateid   ,
            title,
            tradeline,
            uid,
            uname]
    # return (infoid[0],[ i[0] for i in rs])
    return (infoid[0],[ i[0] for i in rs])



sc.textFile('/commit/credit/58/all.iminfo.json*').map(lambda x:parse(x)).filter(lambda x:x!=None).groupByKey().map(lambda (x,y):list(y)[0])\
    .map(lambda x:'\001'.join([ str(i) for i in x])).repartition(20).saveAsTextFile('/user/zlj/tmp/all.iminfo.json_parse')


# /commit/credit/58/fang.info.json


