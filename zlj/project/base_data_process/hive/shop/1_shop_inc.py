# coding:utf-8
__author__ = 'zlj'

# import json
from pyspark.sql import *
import sys
from pyspark import SparkContext
import time

import rapidjson as json

'''
使用脚本
spark-submit  --total-executor-cores  100  --executor-memory 20g  --driver-memory 20g  1_shop_inc.py  -inc    /commit/iteminfo/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-01  20151030 20151101
'''




print()
# /data/develop/ec/tb/iteminfo/jiu.iteminfo


sc = SparkContext(appName="shop-inc")

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

# def valid_jsontxt(content):
#     if type(content) == type(u""):
#         return content.encode("utf-8")
#     else :
#         return content

path = sys.argv[1]


def parse_price(price_dic):
    min = 1000000000.0
    price = '0'
    price_range = '-'
    for value in price_dic:
        tmp = value['price']
        v = 0
        if '-' in tmp:
            v = float(tmp.split('-')[0])
        else:
            v = float(tmp)
        if min > v:
            min = v
            price = v
            if '-' in tmp: price_range = tmp
    return [price, price_range]


def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content


# s=''
# s.split()
def try_parse(line,flag):
    try:
        return parse_shop(line,flag)
    except:return None

def parse_shop(line, flag):
    ts = ''
    txt = ''
    if flag == 'insert':
        txt = valid_jsontxt(line)
    else:
        lis = valid_jsontxt(line).split('\t')
        if len(lis) != 3:
            return
        ts = lis[0]
        txt = lis[2]
    ob = json.loads((txt))
    if type(ob) == type(0.0):
        return None
    itemInfoModel = ob['itemInfoModel']
    location = valid_jsontxt(itemInfoModel.get('location', '-'))
    if 'seller' not in ob.keys():
        return
    seller = ob["seller"]
    evaluateInfo = seller.get("evaluateInfo", [])
    shopId = seller.get("shopId", "-")
    seller_id = seller.get("userNumId", "-")
    seller_name = seller.get("nick", "-")
    credit = seller.get("creditLevel", "-")
    starts = seller.get("starts", "--")
    trackParams = ob['trackParams']
    BC_type = trackParams.get('BC_type', '-')
    item_count = '0'
    for item in seller.get('actionUnits', []):
        if item.has_key('track'):
            if item['track'] == 'Button-AllItem':
                item_count = item.get('value', '0')
    fansCount = seller.get("fansCount", "0")
    try:
        t = seller.get("goodRatePercentage", "0.0").replace('%', '')
        if (t.replace('.', '').isdigit()):
            goodRatePercentage = float(t)
        else:
            goodRatePercentage = 0.0
    except Exception, e:
        print e, line
        return
    # nick= seller.get("nick","--").encode('utf-8')
    weitaoId = seller.get("weitaoId", "--")
    # userNumId = seller.get("userNumId","--")
    # shopTitle = seller.get("shopTitle","--").encode('utf-8')
    shopTitle = seller.get("shopTitle", "--")
    desc_score = evaluateInfo[0].get("score", '0.0')
    service_score = evaluateInfo[1].get("score", '0.0')
    wuliu_score = evaluateInfo[2].get("score", '0.0')
    star = '99'
    list = []
    list.append(shopId)
    list.append(seller_id)
    list.append(shopTitle)
    list.append(seller_name)
    if 'B' in BC_type:
        list.append(star)
    else:
        list.append('0')
    list.append(credit)
    list.append(starts)
    list.append(BC_type)
    list.append(int(item_count))
    list.append(int(fansCount))
    list.append(goodRatePercentage)
    list.append(weitaoId)
    list.append(float(desc_score))
    list.append(float(service_score))
    list.append(float(wuliu_score))
    list.append(location)
    if flag == 'insert':
        list.append(str(int(time.time())))
    else:
        list.append(ts)
    return (shopId, list)


# rdd=sc.textFile('/data/develop/ec/tb/iteminfo_new/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-01',100)\
def fun_sorted(y):
    return sorted(y, key=lambda t: t[-1], reverse=True)[0]


def f_coding(x):
    if type(x) == type(""):
        return x.decode("utf-8")
    else:
        return x


def fun1(x, ds):
    x.append(ds)
    return [f_coding(i) for i in x]


if __name__ == "__main__":
    hiveContext.sql('use wlbase_dev')
    if sys.argv[1] == '-h':
        comment = '-店铺 \n\
			  '
        print comment
        print '-insert argvs:\n argv[1]:file or dir input\n argv[2]:ds  \n'
        print '-inc      argvs:\n argv[1]:file or dir input\n argv[2]:ds_1  \n argv[3] ds\n '
    elif sys.argv[1] == '-insert':
        filepath = sys.argv[2]
        ds = sys.argv[3]
        rdd = sc.textFile(filepath, 100) \
            .map(lambda x: try_parse(x, 'insert')) \
            .filter(lambda x: x is not None) \
            .groupByKey(50).map(lambda (x, y): [i for i in y][0]).map(lambda x: fun1(x, ds))
        df = hiveContext.sql('select * from t_base_ec_shop_dev limit 1')
        schema1 = df.schema
        ddf = hiveContext.createDataFrame(rdd, schema1)
        hiveContext.registerDataFrameAsTable(ddf, 'tmptable')
        sql = '''
        insert overwrite table t_base_ec_shop_dev partition(ds=%s)
        select * from tmptable
        '''
        hiveContext.sql(sql % (ds))
    elif sys.argv[1] == '-inc':
        filepath = sys.argv[2]
        ds_1 = sys.argv[3]
        ds = sys.argv[4]
        rdd = sc.textFile(filepath, 100) \
            .map(lambda x: try_parse(x, 'inc')).filter(lambda x: x is not None) \
            .groupByKey(50).map(lambda (x, y): [i for i in y][0]).map(lambda x: (x[0], x))
        hiveContext.sql('use wlbase_dev')
        df = hiveContext.sql('select * from t_base_ec_shop_dev where ds=%s' % ds_1)
        schema1 = df.schema
        rdd1 = df.map(lambda x: (
        x.shop_id, [x.shop_id, x.seller_id, x.shop_name, x.seller_name, x.star, x.credit, x.starts, x.bc_type,
                    x.item_count, x.fans_count, x.good_rate_p, x.weitao_id, x.desc_score, x.service_score,
                    x.wuliu_score, x.location, x.ts]))
        rdd2 = rdd1.union(rdd).groupByKey()
        rdd3 = rdd2.map(lambda (x, y): fun_sorted(y)).coalesce(40)
        ddf = hiveContext.createDataFrame(rdd3.map(lambda x: fun1(x, ds)), schema1)
        hiveContext.registerDataFrameAsTable(ddf, 'tmptable')
        sql = '''
        insert overwrite table t_base_ec_shop_dev partition(ds=%s)
        select * from tmptable
        '''
        hiveContext.sql(sql % (ds))



    # hiveContext.sql('use wlbase_dev')
    # ds='20151104'
    # df=hiveContext.sql('select * from t_base_ec_item_dev where ds=%s'%ds)
    # schema1=df.schema
    #
    # rdd1=df.map(lambda x:(x.item_id,[x.item_id,x.title,x.cat_id,x.cat_name,x.root_cat_id,x.root_cat_name,x.brand_id,x.brand_name,x.bc_type,x.price,x.price_zone,x.is_online,x.off_time,x.favor,x.seller_id,x.shop_id,x.ts]))
    #
    #
    # rdd3.map(lambda x:"\001".join([str(valid_jsontxt(i)) for i in x])).saveAsTextFile("/data/develop/ec/tb/iteminfo_tmp/1101.dir")

    # return x



# sql='''
# insert overwrite table t_base_ec_item_dev partition(ds=%s)
# select item_id,title,cat_id,cat_name,root_cat_id,root_cat_name,brand_id,brand_name,bc_type,price,price_zone,is_online,off_time,favor,seller_id,shop_id,ts from testtable
# '''
