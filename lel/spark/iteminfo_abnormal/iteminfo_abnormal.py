# coding:utf-8
__author__ = 'lyen'
import sys
import zlib
import base64
import rapidjson as json
from operator import itemgetter


def parse_price(price_dic):
    min = 1000000000.0
    price = '0'
    price_range = '-'
    for value in price_dic:
        tmp = value['price']
        v = ""
        if '-' in tmp:
            v = tmp.split('-')[0]
        else:
            v = tmp
        if v.replace('.', "").isdigit():
            v = float(v)
        else:
            v = 0.1
        if min > v:
            min = v
            price = v
            if '-' in tmp: price_range = tmp
    return [price, price_range]


def decompress(out):
    decode = base64.b64decode(out)
    data = zlib.decompress(decode)
    return data


def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def get_cate_dict(line):
    ss = line.strip().split("\001")
    return (ss[0], [ss[1], ss[3], ss[8]])


def f(line, cate_dict):
    ss = line.strip().split("\t", 2)
    if len(ss) != 3: return None
    item_id = ss[1]
    ts = "1493023724"
    ob = json.loads(valid_jsontxt(ss[2]))
    if type(ob) != type({}): return None
    itemInfoModel = ob.get('itemInfoModel', "\\N")
    if itemInfoModel == "\\N": return None
    location = valid_jsontxt(itemInfoModel.get('location', '\\N'))
    title = itemInfoModel.get('title', '\\N').replace("\n", "")
    favor = itemInfoModel.get('favcount', '0')
    categoryId = itemInfoModel.get('categoryId', '\\N')
    root_cat_id = cate_dict.get(categoryId, ["\\N", "\\N", "\\N"])[1]
    cat_name = cate_dict.get(categoryId, ["\\N", "\\N", "\\N"])[0]
    root_cat_name = cate_dict.get(categoryId, ["\\N", "\\N", "\\N"])[2]

    trackParams = ob.get('trackParams', {})
    BC_type = trackParams.get('BC_type', '\\N')
    if BC_type != 'B' and BC_type != 'C': BC_type = "\\N"
    brandId = trackParams.get('brandId', '\\N')
    brand_name = "\\N"

    value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    price = value[0]
    if int(price) > 160000:
        price = 1.0
    price_zone = value[1]
    seller = ob.get('seller', {})
    seller_id = seller.get('userNumId', '\\N')
    shopId = seller.get('shopId', '\\N')
    off_time = "\\N"
    sku_info = "\\N"
    return (item_id,[item_id,title,categoryId,cat_name,root_cat_id,root_cat_name,brandId,BC_type,str(price),str(price),price_zone,off_time,str(favor),seller_id,shopId,location,sku_info,ts])

def concat(x,y):
    return '\001'.join([valid_jsontxt(i) for i in y])


if __name__ == "__main__":
    if sys.argv[1] == "-local":
        cate_dict = {}
        for line in open("../../public/data/cate_dim"):
            ss = line.strip().split("\001")
            cate_dict[ss[0]] = [ss[1], ss[3], ss[8]]
        for line in sys.stdin:
            print f(line, cate_dict)
    if sys.argv[1] == "-spark":
        from pyspark import SparkContext

        sc = SparkContext(appName="iteminfo_abnormal")
        s = "/commit/iteminfo/tb_iteminfo/*"
        s_dim = "/hive/warehouse/wl_base.db/t_base_ec_dim/ds=20161122/000000_0"
        cate_dict = sc.broadcast(
            sc.textFile(s_dim).map(lambda x: get_cate_dict(x)).filter(lambda x: x != None).collectAsMap()).value
        rdd_c = sc.textFile(s).map(lambda x: f(x, cate_dict)).filter(lambda x: x != None)
        rdd = rdd_c.reduceByKey(lambda a,b:max([a,b], key=itemgetter(-1))).map(lambda (x, y): concat(x, y))
        rdd.saveAsTextFile('/user/lel/temp/iteminfo_abnormal')
