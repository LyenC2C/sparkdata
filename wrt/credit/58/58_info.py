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

sc = SparkContext(appName="weibo_info", conf=conf)

# now_day = sys.argv[1]

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def parse(line):
    ob=json.loads(valid_jsontxt(line))
    if type(ob)!=type({}):return None
    t_action = ob.get('action','-')
    cateid = ob.get('cateid','-')
    catename = ob.get('catename','-')
    decrypted_tel = ob.get('decrypted_tel','-')
    infoid = ob.get('infoid','-')
    # infoimg = ob.get('infoimg','-'),
    # invitation = ob.get('invitation','-'),
    isbiz = ob.get('isbiz','-')
    nickname = ob.get('nickname','-')
    online = ob.get('online','-')
    rootcateid = ob.get('rootcateid','-')
    title = ob.get('title','-')
    tradeline = ob.get('tradeline','-')
    uid = ob.get('uid','-')
    uname = ob.get('uname' ,'-')
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
    return (infoid,rs)


s = "/commit/credit/58/58.iminfo.all.data.20161207"
sc.textFile(s).map(lambda x:parse(x)).filter(lambda x:x!=None).groupByKey().mapValues(list).\
    map(lambda (x,y):"\001".join(valid_jsontxt(i) for i in y[0]))\
    .saveAsTextFile('/user/wrt/temp/58_info')
    # .map(lambda x:'\001'.join([ valid_jsontxt(i) for i in x])).repartition(20).saveAsTextFile('/user/wrt/temp/58_info')

# hfs -rmr /user/wrt/temp/58_info
# spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 58_info.py
# load data inpath '/user/wrt/temp/58_info' overwrite into table wlcredit.t_base_credit_58_info partition(ds = '20161208');
# /commit/credit/58/fang.info.json


