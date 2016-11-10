#coding:utf-8
__author__ = 'wrt'
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

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    return str(res).replace('\n',"").replace("\r","")
'''
data  hdfs:/commit/taobao/shop/shopinfo/info
wiki  http://gitlab.zhishu.com/crawler/datawiki/blob/master/taobao/shop.info.md

店铺额外信息整理
'''

def shopinfo(x):
    ob=json.loads(valid_jsontxt(x))
    if type(ob)==type(1.0):return  None
    shop_age=ob.get('shop_age','-')
    shop_id=ob.get('shop_id','-')
    if len(shop_id)<2:return  None
    user_id=ob.get('user_id','-')
    company_name=ob.get('company_name','-')
    shop_link=ob.get('shop_link','-')
    licence=ob.get('licence','-')
    seller=ob.get('seller','-')
    bail=ob.get('bail','-')
    shop_name=ob.get('shop_name','-')
    return (shop_id,[shop_age,shop_id,user_id,company_name,shop_link,licence,seller,bail,shop_name])


def shoptype(x):
    item_info_list=[]
    ob=json.loads(valid_jsontxt(x))
    if type(ob)==type(1.0):return  None
    shop_id=ob.get('shop_id','-')
    if len(shop_id)<2:return  None
    for k,v  in ob.items():
        if valid_jsontxt(k) != "shop_id":
            item_info_list.append(valid_jsontxt(k).replace(":","").replace(",","") \
                            +":" + valid_jsontxt(v))
    shoptype_info = ",".join(item_info_list)
    return shop_id + "\001" + shoptype_info
    # return (shop_id,shoptype_info)


# rdd1=sc.textFile('/commit/taobao/shop/shopinfo/info').filter(lambda x:'noshop' not in x).map(lambda x:shopinfo(x))

rdd2=sc.textFile('/commit/taobao/shop/shopinfo/type/shop.type.20160602')\
    .filter(lambda x:'noshop' not in x).map(lambda x:shoptype(x)).filter(lambda x:x!=None)

# rdd3=rdd1.join(rdd2)

# rdd3.map(lambda (x,y):y[0]+[y[1]]).map(lambda x:'\001'.join(x)).saveAsTextFile('/commit/taobao/shop/shopinfo_joindata')

rdd2.saveAsTextFile('/user/wrt/temp/shop_type_tmp')


#spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 t_base_shop_type.py
#LOAD DATA  INPATH '/user/wrt/temp/shop_type_tmp' OVERWRITE INTO TABLE t_base_shop_type
