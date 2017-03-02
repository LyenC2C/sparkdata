#coding=utf-8
__author__ = 'wrt'
from pyspark import SparkContext

import rapidjson as json

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkConf
# import rapidjson as json
conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
conf.set("spark.kryoserializer.buffer.mb","1024")
conf.set("spark.akka.frameSize","100")
conf.set("spark.network.timeout","1000s")
conf.set("spark.driver.maxResultSize","8g")


sc = SparkContext(appName="t_xianyu_userinfo.sql",conf=conf)


sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_item_tmp').repartition(6).saveAsTextFile('/user/zlj/tmp/item_title_20161205')


def valid_jsontxt(content):
    res = str(content)
    if type(content) == type(u""):
        res = content.encode("utf-8")
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line):
    result = []
    text = valid_jsontxt(line.strip())
    star = text.find("({")+1
    if star == -1: return None
    end = text.rfind("})") + 1
    ob = json.loads(text[star:end])
    if type(ob)!=type({}):return None
    idleItemSearch = ob.get("idleItemSearch@2",{}).get("data",{})
    totalCount = idleItemSearch.get("totalCount","-")
    userPersonalInfo = ob.get("userPersonalInfo@1",{}).get("data",{})
    if userPersonalInfo == {}: return None
    userId = userPersonalInfo.get("userId","-")
    if userId == "-": return None
    gender = userPersonalInfo.get("gender","-")
    idleUserId = userPersonalInfo.get("idleUserId","-")
    nick = userPersonalInfo.get("nick","-") #闲鱼nick
    tradeCount = userPersonalInfo.get("tradeCount","-")
    tradeIncome = userPersonalInfo.get("tradeIncome","-")
    userNick = userPersonalInfo.get("userNick","-") #淘宝nick
    constellation = userPersonalInfo.get("constellation","-")
    birthday = userPersonalInfo.get("birthday","-")
    city = userPersonalInfo.get("city","-")
    constellation='-' if len(constellation)<1 else constellation
    birthday='-' if len(birthday)<1 else birthday
    cityid='-' if len(city)<1 else city
    result.append(valid_jsontxt(userId))
    result.append(valid_jsontxt(totalCount))
    result.append(valid_jsontxt(gender))
    result.append(valid_jsontxt(idleUserId))
    result.append(valid_jsontxt(nick))
    result.append(valid_jsontxt(tradeCount))
    result.append(valid_jsontxt(tradeIncome))
    result.append(valid_jsontxt(userNick))
    result.append(valid_jsontxt(constellation))
    result.append(valid_jsontxt(birthday))
    result.append(valid_jsontxt(cityid))
    # loc=loc_map.get(cityid,'-')
    loc=''
    province=''
    city=''
    if len(loc.split('-'))==2:
        province ,city=loc.split('-')
    else:
        province =loc
        city='-'
    result.append(valid_jsontxt(province.replace(u'省','')))
    result.append(valid_jsontxt(city.replace(u'市','')))
    return (userId,"\001".join(result))




def f_try(line):
    try:
        return f(line)
    except:return None

s = "/commit/taobao_xianyu_back/"

rdd = sc.textFile(s).map(lambda x:f(x)).filter(lambda x:x!=None).\
    groupByKey().map(lambda (x,y):list(y)[0])
rdd.repartition(100).saveAsTextFile('/user/zlj/temp/xianyu_userinfo_tmp')

