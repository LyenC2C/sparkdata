#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="tb_tm_jiexi_wine")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return
def get_cate_dict(line):
    ss = line.strip().split("\001")
    return (ss[0],[ss[1],ss[3]])

def f(line,cate_dict):
    ss = line.strip().split("\t",2)
    if len(ss) != 3: return None
    txt = valid_jsontxt(ss[2])
    ob = json.loads(txt)
    if type(ob) != type({}): return None
    props = ob.get("props")
    if type(props) != type([]): return None
    itemInfoModel = ob.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
    # categoryId = valid_jsontxt(ob.get("itemInfoModel",{}).get("categoryId","-"))
    categoryId = itemInfoModel.get('categoryId','-')
    # cate_rootid = cate_dict.get(categoryId,["-","-"])[1]
    # if cate_rootid != "50008141": return None
    if categoryId != "50008144" and categoryId != "50013052": return None
    trackParams = ob.get('trackParams',{})
    BC_type = trackParams.get('BC_type','-')
    if BC_type != 'B': return None
    # for ln in props:
    #     # if valid_jsontxt("香型") in valid_jsontxt(ln["name"]):
    #     #     return None
    #     if valid_jsontxt("净含量") == valid_jsontxt(ln["name"]):
    #         return None
    item_id = itemInfoModel.get('itemId','-')
    return item_id

s = "/commit/iteminfo/20160401"
s_dim = "/hive/warehouse/wlbase_dev.db/t_base_ec_dim/ds=20151023/1073988839"
cate_dict = sc.broadcast(sc.textFile(s_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd = sc.textFile(s).map(lambda x: f(x,cate_dict)).filter(lambda x:x!=None)
rdd.saveAsTextFile('/user/wrt/temp/baijiu_0401_itemid')

# spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 40 tb_tm_jiexi.py