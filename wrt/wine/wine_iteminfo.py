#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="wine_iteminfo")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content

def parse_price(price_dic):
    min=1000000000.0
    price='0'
    price_range='-'
    for value in price_dic:
        tmp=value['price']
        v=0
        if '-' in tmp:     v=float(tmp.split('-')[0])
        else :             v=float(tmp)
        if min>v:
            min=v
            price=v
            if '-' in tmp: price_range=tmp
    return [price,price_range]
def get_cate_dict(line):
    ss = line.strip().split("\001")
    return (ss[0],[ss[1],ss[3]])

def f(line,cate_dict):
    ss = line.strip().split("\t",2)
    if len(ss) != 3: return None
    ts = ss[0]
    # item_id = ss[1]
    txt = valid_jsontxt(ss[2])
    ob = json.loads(txt)
    if type(ob) != type({}): return None
    itemInfoModel = ob.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
    location = valid_jsontxt(itemInfoModel.get('location','-'))
    item_id = itemInfoModel.get('itemId','-')
    title = itemInfoModel.get('title','-').replace("\n","")
    favor = itemInfoModel.get('favcount','-')
    categoryId = itemInfoModel.get('categoryId','-')
    cate_rootid = cate_dict.get(categoryId,["-","-"])[1]
    if cate_rootid != "50008141": return None   #非酒类直接舍弃
    cat_name = cate_dict.get(categoryId,["-","-"])[0]
    if categoryId == "50013052" or categoryId == "50008144": #1:白酒 2:红酒 3：啤酒 4:其他酒
        wine_cate = "1"
    elif categoryId == "50013003" or categoryId == "50008143" or categoryId == "50013004" or categoryId == "50512003":
        wine_cate = "2"
    elif categoryId == "50008146" or categoryId == "50013006":
        wine_cate = "3"
    else:
        wine_cate = "4"
    guochan_list = ["50008143","50013004","50013006","123224006","50013006","123214002","50013052","123502002","50008144","50008148","50008147"]
    if categoryId in guochan_list: #进口酒:1 国产酒:2
        is_jinkou = "2"
    else:
        is_jinkou = "1"
    trackParams = ob.get('trackParams',{})
    BC_type = trackParams.get('BC_type','-')
    if BC_type != 'B': return None
    brandId = trackParams.get('brandId','-')
    brand_name = '-'
    xiangxing = "-"
    dushu = "-"
    pinming = "-"
    jinghan = "500ml"
    props=ob.get('props',[])
    for v in props:
        if valid_jsontxt("香型") in valid_jsontxt(v["name"]):
            xiangxing = v['value']
        if valid_jsontxt('品牌') in valid_jsontxt(v['name']):
            brand_name = v['value']
        if valid_jsontxt('度数') == valid_jsontxt(v['name']):
            dushu = filter(str.isdigit, valid_jsontxt(v['value']))[:2]
        if valid_jsontxt('净含量') == valid_jsontxt(v['name']):
            jinghan = v['value']
        if valid_jsontxt('产地') == valid_jsontxt(v['name']):
            if valid_jsontxt('中国') in valid_jsontxt(v['value']):
                is_jinkou = "2"
            else:
                is_jinkou = "1"
        if valid_jsontxt('品名') == valid_jsontxt(v['name']):
            pinming = v['value']
    value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    price = value[0]
    price_zone=value[1]
    seller=ob.get('seller',"-")
    seller_id=seller.get('userNumId','-')
    shopId = seller.get('shopId','-')
    result = []
    # result.append(item_id)
    result.append(title)
    result.append(categoryId)
    result.append(cat_name)
    # result.append(root_cat_id)
    # result.append(root_cat_name)
    result.append(wine_cate)
    result.append(is_jinkou)
    result.append(brandId)
    result.append(brand_name)
    result.append(BC_type)
    result.append(xiangxing)
    result.append(dushu)
    result.append(jinghan)
    result.append(str(price))
    result.append(pinming)
    # result.append((is_online))
    # result.append(off_time)
    result.append(int(favor))
    result.append(seller_id)
    result.append(shopId)
    result.append(location)
    result.append(ts)
    return (item_id,result)

def quchong(x, y):
    max = 0
    item_list = y
    for ln in item_list:
        if int(ln[-1]) > max:
                max = int(ln[-1])
                y = ln
    result = [x] + y
    lv = []
    for ln in result:
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)


s = "/commit/project/wine/3.m.baijiu.only.iteminfo.2016-04-06"
s_dim = "/hive/warehouse/wlbase_dev.db/t_base_ec_dim/ds=20151023/1073988839"
cate_dict = sc.broadcast(sc.textFile(s_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd_c = sc.textFile(s).map(lambda x: f(x,cate_dict)).filter(lambda x:x!=None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
rdd.saveAsTextFile('/user/wrt/temp/baijiu_iteminfo_tmp')

# spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 40 wine_iteminfo.py
