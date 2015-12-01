# encoding: utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

# hanlp 分词 导入库中

def  f(x):
    lv=[]
    s=x[1:len(x)-1].split(',')
    if len(s)!=2:
        return None
    id ,v=x[1:len(x)-1].split(',')
    t=' '.join([i.split('_')[0] for i in v.split() if len(i)>0])
    lv.append(id)
    lv.append(t)
    lv.append(v)

rdd=sc.textFile(sys.argv[1]).map(lambda x:f(x)).filter(lambda x: x is not None)

ds=sys.argv[2]
schema1 = StructType([
    StructField("item_id", StringType(), True),
    StructField("title_cut", StringType(), True),
    StructField("title_cut_seg", StringType(), True)
        ])

df1 = hiveContext.createDataFrame(rdd, schema1)
hiveContext.registerDataFrameAsTable(df1, 'seg_title')
hiveContext.sql('use wlbase_dev')
hiveContext.sql('insert overwrite table t_base_ec_item_title_cut partition(ds=%s) select * from seg_title'%ds)