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

# 标注 子品牌


rdddic=sc.textFile('/user/zlj/wine/wine_brand_list_clean').map(lambda x:x.split('\001')).collect()

dic={}

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content
for k,v in rdddic:
    dic[valid_jsontxt(k)]=[valid_jsontxt(i) for  i in v.split()]

# dic.keys()
# [:1]
def clean(x):
    ss=valid_jsontxt(x)
    son_brand='-'
    if 'big' not in ss: return None
    k,v=ss.split('big')
    kinfo=k.split()
    item_id=kinfo[0].split('\001')[0]
    brand=''.join([i.split('\001')[0] for i in kinfo[1:]])
    rs=[]
    son_brands=[]
    if not dic.has_key(brand): #有些没有品牌 需要去title里找
        vinfo=v.split()
        for i in vinfo[1:-1]:
            w,n=i.split('\001')
            if dic.has_key(w):
                brand=w
                break
    if dic.has_key(brand):
        son_brands=dic[brand]
    else:
        ''
    vinfo=v.split()
    for i in  vinfo[1:-1]:
        w,n=i.split('\001')
        if w in son_brands:
            son_brand=w
        rs.append(w)
    # return rs
    ls=[item_id]
    ls.extend([brand,son_brand])
    ls.extend(rs)
    return ls

def clean_t(x):
    try:
        return clean(x)
    except:return None

name='hive-warehouse-wlservice wuliangy_cut'
# name='tb_wine_title_groupby'

rdd=sc.textFile('/user/zlj/wine/'+name).map(lambda x:clean_t(x))

# rdd=sc.textFile('/user/zlj/wine/tb_wine_title_groupby_cut').map(lambda x:clean(x)).filter(lambda x:x is not None).map(lambda )

rdd.filter(lambda x:x is not None).map(lambda x:'\t'.join(x)).saveAsTextFile('/user/zlj/wine/'+name+'_sonbrand')