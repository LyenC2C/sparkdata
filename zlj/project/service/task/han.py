#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
import time
import rapidjson as json

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


schema = StructType([
    StructField("category_root", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("comment_count", StringType(), True),
    StructField("key", StringType(), True),
    StructField("title", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("category_final", StringType(), True),
    StructField("site", StringType(), True),
    StructField("rank", StringType(), True)]
)

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content

def fun(line_s):
    line=valid_jsontxt(line_s)
    ob=json.loads(line)
    category_root=ob['from_category_root']
    brand=ob['from_brand']
    price=ob['price']
    comment_count=ob['comment_count']
    key=ob.get('key','')
    title=ob['title']
    item_id=ob['item_id']
    category_final=ob['from_category_final']
    site=ob.get('site')
    # site='tmhk'
    rank=ob.get('rank')
    return [category_root,brand,comment_count,price,key,title,item_id,category_final,site,rank]

rdd=sc.textFile('/commit/project/hanguo/han_jingdong').map(lambda x:fun(x))


brand=sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_korea_feedsold_brand_count').map(lambda x:x.split('\001')).map(lambda x:(x[-1],x))


def predict(pre,brand):
    try:
        comment_count=int(pre[2])
        savg=int(float(brand[2]))
        favg=float(brand[3])
        v=0
        if comment_count==0:
            v=  savg
        else :
            v=int(comment_count/favg)
        s=[]
        s.append(v)
        s.extend(pre)
        return s
    except:return None

rs=rdd.map(lambda x:(x[1],x)).repartition(80).join(brand.repartition(80)).map(lambda (x,y):predict(y[0],y[1]))

rs.map(lambda x:'\001'.join([str(i) for i in x])).saveAsTextFile('/commit/project/hanguo/han_jingdong_rs')

rdd=sc.textFile('/commit/project/hanguo/han_yangmatou').map(lambda x:fun(x))


rdd=sc.textFile('/commit/project/hanguo/mia.iteminfo').map(lambda x:fun(x))
rs=rdd.map(lambda x:(x[1],x)).repartition(80).join(brand.repartition(80)).map(lambda (x,y):predict(y[0],y[1]))
rs.map(lambda x:'\001'.join([str(i) for i in x])).saveAsTextFile('/commit/project/hanguo/mia.iteminfo_rs')


name='mia.iteminfo'
rdd=sc.textFile('/commit/project/hanguo2/'+name).map(lambda x:fun(x))
rs=rdd.map(lambda x:(x[1],x)).repartition(80).join(brand.repartition(80)).map(lambda (x,y):predict(y[0],y[1])).filter(lambda x: x is not None)
rs.map(lambda x:'\001'.join([str(i) for i in x])).saveAsTextFile('/commit/project/hanguo2/'+name+'_rs')


name='tmhk.iteminfo'
rdd=sc.textFile('/commit/project/hanguo2/'+name).map(lambda x:fun(x))
rs=rdd.map(lambda x:(x[1],x)).repartition(80).join(brand.repartition(80)).map(lambda (x,y):predict(y[0],y[1])).filter(lambda x: x is not None)
rs.map(lambda x:'\001'.join([str(i) for i in x])).saveAsTextFile('/commit/project/hanguo2/'+name+'_rs')

name='yangmatou.iteminfo'
rdd=sc.textFile('/commit/project/hanguo2/'+name).map(lambda x:fun(x))
rs=rdd.map(lambda x:(x[1],x)).repartition(80).join(brand.repartition(80)).map(lambda (x,y):predict(y[0],y[1])).filter(lambda x: x is not None)
rs.map(lambda x:'\001'.join([str(i) for i in x])).saveAsTextFile('/commit/project/hanguo2/'+name+'_rs')

name='yhd.2.iteminfo'
rdd=sc.textFile('/commit/project/hanguo2/'+name).map(lambda x:fun(x))
rs=rdd.map(lambda x:(x[1],x)).repartition(80).join(brand.repartition(80)).map(lambda (x,y):predict(y[0],y[1])).filter(lambda x: x is not None)
rs.map(lambda x:'\001'.join([str(i) for i in x])).saveAsTextFile('/commit/project/hanguo2/'+name+'_rs')



name='jingdong.iteminfo'
rdd=sc.textFile('/commit/project/hanguo2/'+name).map(lambda x:fun(x))
rs=rdd.map(lambda x:(x[1],x)).repartition(80).join(brand.repartition(80)).map(lambda (x,y):predict(y[0],y[1])).filter(lambda x: x is not None)
rs.map(lambda x:'\001'.join([str(i) for i in x])).saveAsTextFile('/commit/project/hanguo2/'+name+'_rs')



name='yunhou.iteminfo'
rdd=sc.textFile('/commit/project/hanguo2/'+name).map(lambda x:fun(x))
rs=rdd.map(lambda x:(x[1],x)).repartition(80).join(brand.repartition(80)).map(lambda (x,y):predict(y[0],y[1])).filter(lambda x: x is not None)
rs.map(lambda x:'\001'.join([str(i) for i in x])).saveAsTextFile('/commit/project/hanguo2/'+name+'_rs')
# hiveContext.sql('use wlbase_dev')
# df = hiveContext.createDataFrame(rdd, schema)
# hiveContext.registerDataFrameAsTable(df, 't_zlj_korea')