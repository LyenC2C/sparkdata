#coding=utf-8
__author__ = 'wrt'
from pyspark import SparkContext
import sys,rapidjson as json

sc = SparkContext(appName="city_province")

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").\
        replace("\001", "").replace("\\r", "").replace("\r", "")

def get_p_dict(line):
    ss = line.strip().split("\t")
    return (valid_jsontxt(ss[0]),valid_jsontxt(ss[1]))

def f(x,p_dict):
    ob=json.loads(valid_jsontxt(x))
    try:
         uid=ob.get('uid','')
    except:return None
    alipay=ob.get('alipay','-')
    buycnt=ob.get('buycnt','-')
    verify=ob.get('verify','-')
    regtime=ob.get('regtime','-')
    nick=ob.get('nick','-')
    city = j['location']
    location = p_dict.get(valid_jsontxt(city),"") + "\t" + city
    return (uid,[uid,alipay,buycnt,verify,regtime,nick,location])



def quchong(x, y):
    return '\001'.join(y[0])
    # return (x, y)



s_c = "/hive/warehouse/wlbase_dev.db/t_base_ec_tb_userinfo/ds=20160530"
# s = "/commit/taobao/userinfo/*/*" #+ sys.argv[1]
s_p = '/user/wrt/city_pro'
p_dict = sc.broadcast(sc.textFile(s_p).map(lambda x: get_p_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd = sc.textFile('/commit/taobao/userinfo/tbuid*/*').map(lambda x:f(x,p_dict)).filter(lambda x:x  is not None).groupByKey()\
    .map(lambda (x,y):[str(i) for i in list(y)[0]]).map(lambda x:'\001'.join(x))
rdd.saveAsTextFile("/user/wrt/temp/tb_userinfo")

# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 tb_userinfo.py 20160423
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 tb_userinfo.py 20160429
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 tb_userinfo.py 20160518
# LOAD DATA  INPATH '/user/wrt/temp/tb_userinfo' INTO TABLE t_base_ec_tb_userinfo PARTITION (ds='20160530');
#始终使用20160530这个分区,其他分区无效