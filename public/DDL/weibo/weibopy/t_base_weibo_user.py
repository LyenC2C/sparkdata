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
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def parse(line):
    txt = valid_jsontxt(line.strip())
    ob=json.loads(txt)
    if type(ob)!=type({}):return None
    id=ob.get('id','-1')
    if  int(id)<-1:return None
    idstr=ob.get('idstr','-')
    screen_name=ob.get('screen_name','-')
    name=ob.get('name','-')
    province=ob.get('province','-1')
    city=ob.get('city','-1')
    location=ob.get('location','-')
    description=ob.get('description','-')
    url=ob.get('url','-')
    profile_image_url=ob.get('profile_image_url','-')
    profile_url=ob.get('profile_url','-')
    domain=ob.get('domain','-')
    weihao=ob.get('weihao','-')
    gender=ob.get('gender','-')
    followers_count=ob.get('followers_count','-1')
    friends_count=ob.get('friends_count','-1')
    statuses_count=ob.get('statuses_count','-1')
    favourites_count=ob.get('favourites_count','-')
    created_at=ob.get('created_at','-')
    following=ob.get('following','-')
    allow_all_act_msg=ob.get('allow_all_act_msg','-')
    geo_enabled=ob.get('geo_enabled','-')
    verified=ob.get('verified','-')
    verified_type=ob.get('verified_type','-1')
    remark=ob.get('remark','-')
    # status=ob.get('status','-')
    allow_all_comment=ob.get('allow_all_comment','-')
    avatar_large=ob.get('avatar_large','-')
    avatar_hd=ob.get('avatar_hd','-')
    verified_reason=ob.get('verified_reason','-')
    follow_me=ob.get('follow_me','-')
    online_status=ob.get('online_status','-1')
    bi_followers_count=ob.get('bi_followers_count','-')
    lang=ob.get('lang','-')
    rs=[id,idstr,screen_name,name,province,city,location,description,url,profile_image_url,profile_url,domain,weihao,gender,followers_count,friends_count,statuses_count,favourites_count,created_at,following,allow_all_act_msg,geo_enabled,verified,verified_type,remark,allow_all_comment,avatar_large,avatar_hd,verified_reason,follow_me,online_status,bi_followers_count,lang]
    return (id,'\001'.join([ str(i) for i in rs] ))


def try_parse(line):
    try:
        return  parse(line)
    except:
        return None

# sc.textFile('/data/develop/sinawb/user_info.json.20160401').map(lambda x:parse(x)).filter(lambda x:x!=None).groupByKey().map(lambda (x,y):list(y)[0])\
#     .map(lambda x:'\001'.join([ str(i) for i in x])).saveAsTextFile('/user/zlj/tmp/sinawb_user_info.json.20160401')



sc.textFile('/commit/weibo/userinfo/*/*').map(lambda x:parse(x)).filter(lambda x:x!=None).\
    groupByKey().map(lambda (x,y):list(y)[0]).saveAsTextFile('/user/wrt/temp/userinfo_all')


# LOAD DATA   INPATH '/user/zlj/tmp/sinawb_user_info.json.20160401' OVERWRITE INTO TABLE t_base_weibo_user PARTITION (ds='20160829')
# LOAD DATA   INPATH '/user/zlj/tmp/sinawb_user_info.json.20161101' OVERWRITE INTO TABLE t_base_weibo_user PARTITION (ds='20161101')
#LOAD DATA   INPATH '/user/wrt/temp/userinfo_all' OVERWRITE INTO TABLE t_base_weibo_user_new PARTITION (ds='20161120')