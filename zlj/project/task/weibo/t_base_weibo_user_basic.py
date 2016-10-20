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


def fun_line(line):
    ob = json.loads(valid_jsontxt(line))
    if type(ob)!=type({}):return None
    birthday =ob.get('birthday','-')
    birthday_visible =ob.get('birthday_visible','-')
    city =ob.get('city','-')
    completion_level =ob.get('completion_level','-')
    created_at =ob.get('created_at','-')
    credentials_num =ob.get('credentials_num','-')
    credentials_type =ob.get('credentials_type','-')
    default_avatar =ob.get('default_avatar','-')
    description =ob.get('description','-')
    domain =ob.get('domain','-')
    email =ob.get('email','-')
    email_visible =ob.get('email_visible','-')
    gender =ob.get('gender','-')
    id =ob.get('id','-')
    lang =ob.get('lang','-')
    location =ob.get('location','-')
    msn =ob.get('msn','-')
    msn_visible =ob.get('msn_visible','-')
    name =ob.get('name','-')
    profileImageUrl =ob.get('profileImageUrl','-')
    province =ob.get('province','-')
    qq =ob.get('qq','-')
    qq_visible =ob.get('qq_visible','-')
    real_name =ob.get('real_name','-')
    real_name_visible =ob.get('real_name_visible','-')
    screen_name =ob.get('screen_name','-')
    url =ob.get('url','-')
    url_visible =ob.get('url_visible','-')
    return [id, birthday,
    birthday_visible,
    city,
    completion_level,
    created_at,
    credentials_num,
    credentials_type,
    default_avatar,
    description,
    domain,
    email,
    email_visible,
    gender,
    lang,
    location,
    msn,
    msn_visible,
    name,
    profileImageUrl,
    province,
    qq,
    qq_visible,
    real_name,
    real_name_visible,
    screen_name,
    url,
    url_visible]


sc.textFile('/commit/user_basic.json').map(lambda x:fun_line(x)).filter(lambda x:x!=None).\
    map(lambda x:'\001'.join([str(i) for i in x])).saveAsTextFile('/user/zlj/tmp/user_basic')