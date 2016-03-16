#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
# import rapidjson as json

sc = SparkContext(appName="user_cattags")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
a = {
    '50018222': '理工男	数码发烧友	数码控',
    '50007218': '行政	office',
    '50012082': '爱下厨',
    '124044001': '数码控',
    '50022703': '数码控',
    '50008097': '数码控',
    '50019780': '数码控',
    '50011972': '数码控',
    '1512': '数码控',
    '14': '文青	爱摄影',
    '124242008': '数码发烧友	数码控',
    '50018004': '学习',
    '20': '游戏达人',
    '11': '数码控',
    '1101': '数码控',
    '33': '爱读书',
    '34': '爱音乐	爱生活',
    '50017300': '音乐	品质',
    '50017908': '彩迷',
    '50023722': '近视	美妆达人',
    '50016349': '下厨房',
    '50016348': '持家',
    '122852001': '持家',
    '21': '持家',
    '50008163': '持家',
    '122928002': '持家',
    '50025705': '持家',
    '122950001': '送礼',
    '122952001': '下厨房',
    '50020485': '理工男	动手能力强',
    '50008164': '有房',
    '124050001': '有房',
    '50020611': '行政',
    '50020332': '有房',
    '124568010': '有房',
    '50020808': '品质	生活',
    '27': '有房',
    '50020857': '品质	手工爱好者',
    '1625': '需细分',
    '16': '需细分',
    '50006843': '需细分',
    '50010404': '需细分',
    '50011740': '需细分',
    '30': '需细分',
    '3009': '需细分',
    '35': '细分',
    '50022517': '待产',
    '50014812': '婴儿',
    '25': '幼儿',
    '50008165': '有孩儿	送礼',
    '122650005': '有孩儿	送礼',
    '124468001': '三农',
    '124466001': '三农',
    '124470001': '三农',
    '50016891': '游戏达人',
    '50011665': '游戏达人',
    '99': '游戏达人',
    '40': '企鹅粉',
    '28': '品质男',
    '23': '收藏家',
    '124484008': '二次元',
    '50468001': '高品质',
    '50008171': '高品质',
    '50011397': '高品质',
    '1705': '爱美',
    '50013864': '爱美',
    '50026523': '生活',
    '50050471': '结婚',
    '29': '萌宠',
    '50025707': '旅游',
    '2813': '嘿嘿嘿',
    '50014927': '活到老学到老',
    '50454031': '旅游',
    '50025111': 'O2O',
    '50011949': '旅游	嘿嘿嘿',
    '50025110': '享受生活',
    '50026555': '持家',
    '50019095': '持家',
    '50008075': '吃货',
    '50007216': '品质生活',
    '50002768': '养生	爱美',
    '50010788': '爱美',
    '50023282': '爱美',
    '1801': '爱美',
    '50074001': '机车党',
    '26': '有车',
    '50013886': '户外',
    '124354002': '电驴一族',
    '122684003': '户外',
    '50010728': '健身	运动',
    '50510002': '户外',
    '50011699': '运动',
    '50010388': '运动',
    '50012029': '运动',
    '50020275': '养生	保健',
    '50008825': '养生	保健',
    '50026800': '养生	保健',
    '50026316': '小资',
    '50050359': '品质生活',
    '50012472': '持家',
    '50016422': '持家',
    '124458005': '爱茶',
    '50008141': '爱酒',
    '50002766': '吃货'
}
import math


def f(x):
    if ( str(x.price).replace('.', '').isdigit()):
        price=float(x.price)
        root_cat_id=x.root_cat_id
        user_id=x.user_id
        if price<1.1:return None
        score=round(math.log(price),2)
        if not a.has_key(root_cat_id):return None
        tags=a.get(root_cat_id).decode('utf-8')
        lv=[]
        for i in tags.split():
            lv.append((user_id+"_"+i,score))
        return lv
    else :return None

# path='/hive/warehouse/wlbase_dev.db/t_zlj_t_base_ec_item_feed_dev_2015_iteminfo_t/'
# rdd=sc.textFile(path).map(lambda x:f(x)).filter(lambda x: x is not None).flatMap(lambda x:x)
hiveContext.sql('use wlbase_dev')
rdd=hiveContext.sql('select user_id,root_cat_id,price, ds from t_base_ec_record_dev_s')\
    .map(lambda x:f(x)).filter(lambda x: x is not None).flatMap(lambda x:x)
rdd1=rdd.reduceByKey(lambda a,b:a+b).map(lambda (x,score):(x.split('_')[0],x.split('_')[1]+"_"+str(score)))

def sort_limit(y):
    lv=[]
    for i in y:
        lv.append(i.split('_'))
    ts=[i for index, i in enumerate(sorted(lv, key=lambda t: float(t[-1]), reverse=True)) if index < 11]
    return ' '.join([i[0]+'_'+i[1] for i in ts])

rdd2=rdd1.groupByKey().map(lambda (x,y):(x,sort_limit(y))).repartition(100)
schema1 = StructType([
    StructField("user_id", StringType(), True),
    StructField("cat_tags", StringType(), True),
 ])

hiveContext.sql('use wlbase_dev')
df=hiveContext.createDataFrame(rdd2,schema1)
hiveContext.registerDataFrameAsTable(df, 'tmptable')
hiveContext.sql('drop table if EXISTS  t_zlj_userbuy_item_cattags')
hiveContext.sql('create table t_zlj_userbuy_item_cattags as select * from tmptable')

# rdd2.saveAsTextFile('/user/zlj/temp/tags')

 # round(log2(cast(t1.price as FLOAT))*pow(0.5, (datediff)/4.0)*50,4) AS score