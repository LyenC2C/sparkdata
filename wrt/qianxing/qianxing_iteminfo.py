#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext

sc = SparkContext(appName="qianxing_iteminfo")
today = sys.argv[1]
yesterday = sys.argv[2]
hiveContext = HiveContext(sc)
sqlContext = SQLContext(sc)


def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")


def f(line):
    ss = line.strip().split("\t",2)
    if len(ss) !=   3: return None
    item_id = ss[1]
    ts = ss[0]
    ob = json.loads(valid_jsontxt(ss[2]))
    if type(ob) != type({}):return None
    seller = ob.get('seller',{})
    shopId = seller.get('shopId','-')
    if shopId != '68907524': return None
    itemInfoModel = ob.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
    title = itemInfoModel.get('title','-').replace("\n","").decode("utf-8")
    result = []
    result.append(item_id)
    result.append(title)
    result.append(ts)
    return (item_id,result)

def quchong(x, y):
    max = 0
    item_list = y
    for ln in item_list:
        if int(ln[-1]) > max:
                max = int(ln[-1])
                y = ln
    result = y
    return result
    # return [valid_jsontxt(ln) for ln in result]



s1 = "/commit/iteminfo/shopid_128287536/item.info." + today
rdd_c = sc.textFile(s1).map(lambda x: f(x)).filter(lambda x:x!=None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
schema = StructType([
           StructField("item_id", StringType(), True),
           StructField("title",StringType(), True),
           StructField("ts",StringType(), True)])

df=hiveContext.createDataFrame(rdd,schema)
hiveContext.registerDataFrameAsTable(df,'qianxing_iteminfo')
# hiveContext.sql("insert overwrite table wl_base.t_base_qianxing_iteminfo partition (ds =" + today + ")\
# select * from qianxing_iteminfo")
sql_merge = '''
insert overwrite table wl_base.t_base_qianxing_iteminfo partition (ds =''' + today + ''')
select
COALESCE(t1.item_id,t2.item_id),
COALESCE(t1.title,t2.title),
COALESCE(t1.ts,t2.ts)
from
qianxing_iteminfo t1
full join
(select * from  wl_base.t_base_qianxing_iteminfo where ds =''' + yesterday +''')t2
on
t1.item_id = t2.item_id
'''
hiveContext.sql(sql_merge)
# rdd.saveAsTextFile('/user/wrt/temp/iteminfo_tmp')


# hfs -rmr /user/wrt/temp/iteminfo_tmp
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 100 qianxing_iteminfo.py 20170511
#LOAD DATA  INPATH '/user/wrt/temp/iteminfo_tmp' OVERWRITE INTO TABLE t_base_ec_item_dev_new PARTITION (ds='20160606');