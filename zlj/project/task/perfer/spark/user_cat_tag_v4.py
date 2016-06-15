#coding:utf-8
__author__ ='zlj'
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
 '124470006':'A',
'50024971':'A',
'26':'A',
'124354002':'B',
'50074001':'B',
'122684003':'B',
'50008165':'C',
'122650005':'C',
'123302001':'D',
'50020485':'D',
'27':'D',
'50008164':'D',
'124050001':'D',
'2128':'D',
'124568010':'D',
'50020808':'D',
'50023804':'D',
'122852001':'D',
'124698018':'D',
'121536003':'E',
'50011665':'E',
'50016891':'E',
'122870005':'E',
'20':'E',
'124484008':'E',
'50026523':'E',
'50012029':'F',
'50010728':'F',
'50510002':'F',
'50011699':'F',
'50025110':'F',
'50013886':'F',
'29':'G',
'50025004':'G',
'50023575':'G',
'50025705':'G',
'50007216':'G',
'50014812':'G',
'2813':'G',
'34':'G',
'50023878':'G',
'124164002':'G',
'50012082':'G',
'50006842':'G',
'33':'G',
'16':'G',
'50019095':'G',
'21':'G',
'122696005':'G',
'50050471':'G',
'50025707':'G',
'50022703':'G',
'50025111':'G',
'50012100':'G',
'50002768':'G',
'50016348':'G',
'122662002':'G',
'50008163':'G',
'122718004':'G',
'50026555':'H',
'50024451':'H',
'50024612':'H',
'50016422':'H',
'124868003':'H',
'35':'H',
'124458005':'H',
'50002766':'H',
'50020275':'H',
'50050359':'H',
'50016349':'H',
'50008075':'H',
'122952001':'H',
'50026316':'H',
'50008141':'H',
'50026800':'H',
'23':'I',
'28':'I',
'50011397':'I',
'50468001':'I',
'50013864':'I',
'50020611':'J',
'124466011':'J',
'50007218':'J',
'122686003':'J',
'50014811':'J',
'124116010':'J',
'50020332':'J'
}
import math


def f(x):
    if ( str(x.price).replace('.','').isdigit()):
        price=float(x.price)
        root_cat_id=str(x.root_cat_id)
        user_id=x.user_id
        if price<1.1:return None
        score=round(math.log(price),2)
        # print root_cat_id,a
        if not a.has_key(root_cat_id):return None
        tags=a.get(root_cat_id).decode('utf-8')
        lv=[]
        for i in tags.split():
            lv.append((user_id+"_"+i.strip(),score))
        return lv
    else :return None

# path='/hive/warehouse/wlbase_dev.db/t_zlj_t_base_ec_item_feed_dev_2015_iteminfo_t/'
# rdd=sc.textFile(path).map(lambda x:f(x)).filter(lambda x: x is not None).flatMap(lambda x:x)
hiveContext.sql('use wlbase_dev')
rdd=hiveContext.sql('select user_id,root_cat_id,price  from  t_zlj_ec_userbuy ')\
    .repartition(150).map(lambda x:f(x)).filter(lambda x: x is not None).flatMap(lambda x:x)
rdd1=rdd.repartition(150).reduceByKey(lambda a,b:a+b).map(lambda (x,score):(x.split('_')[0],x.split('_')[1]+"_"+str(score)))

def sort_limit(y):
    lv=[]
    for i in y:
        lv.append(i.split('_'))
    ts=[(i[0],float(i[1])) for index, i in enumerate(sorted(lv, key=lambda t: float(t[-1]), reverse=True)) if index < 11]
    v=sum([v for k,v in ts])
    return''.join([i[0]+'_'+str(round(i[1]/v,4)) for i in ts])

rdd2=rdd1.groupByKey().map(lambda (x,y):(x,sort_limit(y))).repartition(100)
schema1 = StructType([
    StructField("user_id", StringType(), True),
    StructField("cat_tags", StringType(), True),
 ])

hiveContext.sql('use wlbase_dev')
df=hiveContext.createDataFrame(rdd2,schema1)
hiveContext.registerDataFrameAsTable(df,'tmptable')
hiveContext.sql('drop table if EXISTS  t_zlj_userbuy_item_cattags_wuzong')
hiveContext.sql('create table t_zlj_userbuy_item_cattags_wuzong as select * from tmptable')

# rdd2.saveAsTextFile('/user/zlj/temp/tags')

# round(log2(cast(t1.price as FLOAT))*pow(0.5, (datediff)/4.0)*50,4) AS score