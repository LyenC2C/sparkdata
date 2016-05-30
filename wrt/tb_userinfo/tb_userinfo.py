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
    return (ss[0],ss[1])

def map_line(line,p_dict):
    try:
        j = json.loads(valid_jsontxt(line.strip()))
        #{"alipay": "已通过支付宝实名认证", "uid": "12871760", "buycnt": "1520", "verify": "VIP等级5", "regtime": "2005.03.10", "nick": "happle_1999", "location": "北京"}
        alipay = '0' if j['alipay'] == None else '1'
        uid = j['uid']
        buycnt = j["buycnt"]
        verify = j['verify']
        regtime = j['regtime']
        nick = j['nick']
        location = j['location']
        location = p_dict.get(valid_jsontxt(location),"") + "\t" + location
        return '\001'.join([uid,alipay,buycnt,verify,regtime,nick,location])
    except:
        pass

# def quchong(line):


s_c = ""
s = "/commit/taobao/userinfo/tbuid." + sys.argv[1]
s_p = '/user/wrt/city_pro'
p_dict = sc.broadcast(sc.textFile(s_p).map(lambda x: get_p_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd = sc.textFile(s).map(lambda x:map_line(x,p_dict)).filter(lambda x:x!=None)
# rdd_c = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_ec_tb_userinfo/ds=20160530").map(lambda x:x.splite("\001")[0])
rdd.saveAsTextFile("/user/wrt/temp/tb_userinfo")

# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 tb_userinfo.py 20160423
# LOAD DATA  INPATH '/user/wrt/temp/tb_userinfo' OVERWRITE INTO TABLE t_base_ec_tb_userinfo PARTITION (ds='20160530');
#始终使用20160530这个分区,其他分区无效