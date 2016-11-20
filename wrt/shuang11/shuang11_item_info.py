#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="shuang11_itemid_info")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def get_cate_dict(line):
    ss = line.strip().split("\001")
    return (ss[0],[ss[1],ss[3],ss[8]])


def f(line,cate_dict):
    ss = line.strip().split("\t")
    if len(ss) != 3: return None
    ts = ss[0]
    item_id = ss[1]
    txt = valid_jsontxt(ss[2])
    ob = json.loads(txt)
    if type(ob) == type(1.0): return None
    seller = ob.get("seller",[])
    itemInfoModel = ob.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
    title = itemInfoModel.get('title','-').replace("\n","")
    categoryId = itemInfoModel.get('categoryId','-')
    root_cat_id = cate_dict.get(categoryId,["-","-","-"])[1]
    cat_name = cate_dict.get(categoryId,["-","-","-"])[0]
    root_cat_name = cate_dict.get(categoryId,["-","-","-"])[2]
    location = valid_jsontxt(itemInfoModel.get('location', '-').replace("省",""))
    trackParams = ob.get('trackParams',{})
    brand_name = "-"
    props = ob.get('props',[])
    for v in props:
        if valid_jsontxt('品牌') == valid_jsontxt(v.get('name',"-")) and brand_name == "-" :
            brand_name = v.get('value',"-")
    shopId = seller.get('shopId','-')
    shopTitle = seller.get("shopTitle", "--")
    evaluateInfo = seller.get("evaluateInfo", [])
    if len(evaluateInfo) < 3: evaluateInfo =[{},{},{}]
    if type(evaluateInfo[0]) == type({}):
        desc_score = evaluateInfo[0].get("score", '0.0').strip()
        desc_highGap = evaluateInfo[0].get("highGap", '0.0').strip()
    else:
        desc_score = '0.0'
        desc_highGap = '0.0'
    if type(evaluateInfo[1]) == type({}):
        service_score = evaluateInfo[1].get("score", '0.0').strip()
        service_highGap = evaluateInfo[1].get("highGap", '0.0').strip()
    else:
        service_score = '0.0'
        service_highGap = '0.0'
    if type(evaluateInfo[2]) == type({}):
        wuliu_score = evaluateInfo[2].get("score", '0.0').strip()
        wuliu_highGap = evaluateInfo[2].get("highGap", '0.0').strip()
    else:
        wuliu_score = '0.0'
        wuliu_highGap = '0.0'
    if not desc_score.replace(".","").isdigit(): desc_score = '0.0'
    if not service_score.replace(".","").isdigit(): service_score = '0.0'
    if not wuliu_score.replace(".","").isdigit(): wuliu_score = '0.0'
    if not desc_highGap.replace(".","").replace("-","").isdigit(): desc_highGap = '0.0'
    if not service_highGap.replace(".","").replace("-","").isdigit(): service_highGap = '0.0'
    if not wuliu_highGap.replace(".","").replace("-","").isdigit(): wuliu_highGap = '0.0'
    if not shopId.isdigit(): return None
    result = []
    result.append(item_id)
    result.append(title)
    result.append(categoryId)
    result.append(cat_name)
    result.append(root_cat_id)
    result.append(root_cat_name)
    result.append(shopId)
    result.append(shopTitle)
    result.append(desc_score)
    result.append(service_score)
    result.append(wuliu_score)
    result.append(desc_highGap)
    result.append(service_highGap)
    result.append(wuliu_highGap)
    result.append(location)
    result.append(ts)
    return (item_id,result)
    # return "\001".join([str(valid_jsontxt(i)) for i in result])
def quchong(x, y):
    max = 0
    item_list = y
    for ln in item_list:
        if int(ln[-1]) > max:
                max = int(ln[-1])
                y = ln
    result = y
    lv = []
    for ln in result:
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)

s1 = "/commit/iteminfo/20161110"
s2 = "/commit/iteminfo/20161111"
s3 = "/commit/iteminfo/20161112"
rdd1 = sc.textFile(s1)
rdd2 = sc.textFile(s2)
rdd3 = sc.textFile(s3)
rdd = rdd1.union(rdd2).union(rdd3)
c_dim = "/hive/warehouse/wlbase_dev.db/t_base_ec_dim/ds=20151023/1073988839"
cate_dict = sc.broadcast(sc.textFile(c_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd_c = rdd.map(lambda x:f(x,cate_dict)).filter(lambda x:x!=None)
rdd_c.groupByKey().mapValues(list).map(lambda (x,y): quchong(x,y))\
    .saveAsTextFile("/user/wrt/temp/shuang11_iteminfo")


# hfs -rmr /user/wrt/temp/shuang11_iteminfo
# spark-submit  --executor-memory 6G   --driver-memory 8G  --total-executor-cores 80  shuang11_item_info.py
# LOAD DATA  INPATH '/user/wrt/temp/shuang11_iteminfo' OVERWRITE INTO TABLE wlservice.t_wrt_tmp_shuang11_iteminfo;