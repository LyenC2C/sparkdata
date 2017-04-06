#coding:utf-8
__author__ = 'wrt'

import sys
import rapidjson as json
from pyspark import SparkContext

# today = sys.argv[1]
# yesterday = sys.argv[2]

sc = SparkContext(appName="t_base_unusual_iteminfo_"+today)

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def get_cate_dict(line):
    ss = line.strip().split("\001")
    return (valid_jsontxt(ss[0]),[ss[1],ss[3],ss[8]])

def f1(line,cate_dict):
    ss = line.strip().split("\t",5)
    ts = ss[0]
    rec_itemid = ss[1]
    rec_sellerid = ss[2]
    rec_catid = ss[3]
    app_id = ss[4] #app_id : 看了该宝贝的人还看了； 11 买了该宝贝还买了 ； 3066 : 邻家好货
    text = ss[5]
    ob = json.loads(valid_jsontxt(text))
    result = ob.get("result",[])
    res = []
    if result == []: return [None]
    for item in result:
        lv = []
        rec = []
        item_id = item.get("itemId",'\\N')
        if item_id == '\\N': continue
        seller_id = item.get("userId",'\\N')
        title = item.get("itemName",'\\N')
        picurl = item.get("pic",'\\N')
        price = item.get('price','\\N')
        promotionPrice = item.get('promotionPrice','\\N')
        sold = item.get('sold','\\N')
        score = item.get('score','\\N')
        categoryId = str(item.get('categoryId','\\N'))
        root_cat_id = item.get('cat1','\\N')
        cat_name = cate_dict.get(categoryId,['\\N','\\N','\\N'])[0]
        root_cat_name = cate_dict.get(categoryId,['\\N','\\N','\\N'])[2]
        rec.append(rec_itemid)
        rec.append(rec_sellerid)
        rec.append(rec_catid)
        rec.append(app_id)
        rec_str = "_".join(rec)
        lv.append(item_id)
        lv.append(rec_str)
        lv.append(seller_id)
        lv.append(title)
        lv.append(picurl)
        lv.append(price)
        lv.append(promotionPrice)
        lv.append(sold)
        lv.append(score)
        lv.append(categoryId)
        lv.append(cat_name)
        lv.append(root_cat_id)
        lv.append(root_cat_name)
        lv.append(ts)
        res.append(lv)
    return res


# def quchong(x,y):
#     max = 0
#     item_list = y
#     for ln in item_list:
#         if int(ln[-1]) > max:
#             max = int(ln[-1])
#             y = ln
#     return (x,y)

# def merge_res(a,b):
#     return max([a,b],lambda x:x[-1])

def merge_rec_str(a,b):
    rec_str_merge = valid_jsontxt(a[1] + "\002" + b[1])
    result = [a[0]] + [rec_str_merge] + a[2:]
    return result
    # return "\001".join([valid_jsontxt(ln) for ln in result])


s_dim = "/hive/warehouse/wl_base.db/t_base_ec_dim/ds=20161122/000000_0"
s1 = "/commit/taobao/recomment/recommend.info*"
# s2 = "/hive/warehouse/wl_base.db/t_base_ec_shopitem_b/ds=" + yesterday

cate_dict = sc.broadcast(sc.textFile(s_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd1_c = sc.textFile(s1).flatMap(lambda x:f1(x,cate_dict)).filter(lambda x:x != None) #解析
rdd1 = rdd1_c.map(lambda x:((x[0],x[1]),x)).reduceByKey(lambda a,b:max([a,b],key=lambda x:x[-1]))\
    .map(lambda (k,v):(str(v[0]),v))#用ts最大值去重
# rdd = rdd1.map(lambda x:(x[0],x))\
rdd = rdd1.reduceByKey(lambda a,b:merge_rec_str(a,b))\
    .map(lambda (k,v):"\001".join([valid_jsontxt(ln) for ln in v]))
#.reduce
rdd.saveAsTextFile('/user/wrt/temp/t_base_unusual_iteminfo')
# rdd.filter(lambda x:len(x.split("\001")) != 7).saveAsTextFile('/user/wrt/shopitem_b_error')
#hfs -rmr /user/wrt/temp/t_base_unusual_iteminfo
#LOAD DATA  INPATH '/user/wrt/temp/t_base_unusual_iteminfo' OVERWRITE INTO TABLE wl_base.t_base_unusual_iteminfo PARTITION (ds='20170329');