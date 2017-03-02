#coding:utf-8
__author__ = 'wrt'

import sys
import rapidjson as json
from pyspark import SparkContext

today = sys.argv[1]
yesterday = sys.argv[2]

sc = SparkContext(appName="t_base_shopitem_b_"+today)

def valid_jsontxt(content):

    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f1(line):
    ss = line.strip().split("\t",4)
    shop_id = ss[1]
    ts = ss[0]
    text = ss[4]
    if text == "noshop": return [None]
    ob = json.loads(valid_jsontxt(text))
    if type(ob) != type([]): return [None]
    result = []
    for item in ob:
        lv = []
        item_id = item.get("item_id","-")
        # if item_id == None or item_id == 'None': item_id = '-'
        if not valid_jsontxt(item_id).isdigit(): continue
        # title = item.get("title","-")
        # picUrl = item.get("picUrl","-")
        sold = item.get("sold","-")
        # reservePrice = item.get("reservePrice","-")
        # if reservePrice == "": reservePrice = "-"
        salePrice = item.get("salePrice","-")
        up_day = today #默认为今日新上架，后面会进行调整
        down_day = "0"
        lv.append(valid_jsontxt(shop_id))
        lv.append(valid_jsontxt(item_id))
        lv.append(valid_jsontxt(sold))
        lv.append(valid_jsontxt(salePrice))
        lv.append(valid_jsontxt(up_day)) #上架日期
        lv.append(valid_jsontxt(down_day)) #0代表上架，日期代表下架日期
        lv.append(valid_jsontxt(ts))
        result.append((item_id,lv))
    return result

def f2(line):
    ss = line.strip().split('\001')
    if len(ss) != 7: return None
    if not ss[1].isdigit(): return None
    ss.append(yesterday) #强行增加一个字段，可以理解为ds使得昨日字段列表的长度变成8，好与今日的数据区分开
    return (ss[1],ss)

def quchong(x,y):
    max = 0
    item_list = y
    for ln in item_list:
        if int(ln[-1]) > max:
            max = int(ln[-1])
            y = ln
    return (x,y)

def twodays(x,y):   #同一个item_id下进行groupby后的结果
    item_list = y
    if len(item_list) == 1: #只有一个商品
        if len(item_list[0]) == 8:
            yes_item = item_list[0] #此商品为昨日商品（昨日商品多个ds字段），今日商品需要复制昨日商品
            if yes_item[5] == '0': #此昨日商品在今天之前没下架，但是今天下架了;否则是今天之前就下架了，那么复制即可
                yes_item[5] = today #设置down_day为今日日期
            result = yes_item[:-1]   #记得将最后的ds去掉，不要复制进来
        if len(item_list[0]) == 7:
            tod_item = item_list[0] #此商品为今日商品，说明此商品今天上架，此前没出现过
            result = tod_item #使用默认值即可
    elif len(item_list) == 2: #有两个商品，一个是昨日，一个是今日
        #判断今日和昨日的位置并分别命名赋值
        if len(item_list[0]) == 8:
            yes_item = item_list[0]
            tod_item = item_list[1]
        if len(item_list[0]) == 7:
            tod_item = item_list[0]
            yes_item = item_list[1]
        tod_item[4] = yes_item[4] #无论此商品在曾经是否下过架，今天都已经上架了，那么复制他的上架时间即可
        if not tod_item[2].isdigit(): #今日商品销量为null，直接复制昨日商品销量
            tod_item[2] = yes_item[2]
        elif not yes_item[2].isdigit(): #今日商品销量为一个数值，且昨日商品销量为null时，不复制昨日商品
            tod_item[2] = tod_item[2]
        elif int(tod_item[2]) < int(yes_item[2]): #今日和昨日商品销量皆为数值且今日销量小于昨日销量时，今日销量需复制昨日销量
            tod_item[2] = yes_item[2]
        result = tod_item
    # else: return '\001'.join([str(valid_jsontxt(i)) for i in item_list])
    return "\001".join([str(valid_jsontxt(i)) for i in result])


today = sys.argv[1]
yesterday = sys.argv[2]


s1 = "/commit/shopitem_b/" + today
s2 = "/hive/warehouse/wl_base.db/t_base_ec_shopitem_b/ds=" + yesterday

rdd1_c = sc.textFile(s1).flatMap(lambda x:f1(x)).filter(lambda x:x != None) #解析
rdd1 = rdd1_c.groupByKey().mapValues(list).map(lambda (x, y):quchong(x, y)) #去重
rdd2 = sc.textFile(s2).map(lambda x:f2(x)).filter(lambda x:x != None) #导入昨天数据
rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x, y):twodays(x, y)) #两天数据合并
rdd.saveAsTextFile('/user/wrt/shopitem_tmp')
# rdd.filter(lambda x:len(x.split("\001")) != 7).saveAsTextFile('/user/wrt/shopitem_b_error')
#hfs -rmr /user/wrt/shopitem_tmp
#spark-submit  --executor-memory 9G  --driver-memory 8G  --total-executor-cores 120 t_base_shopitem_b.py 20161112 20161111
#LOAD DATA  INPATH '/user/wrt/shopitem_tmp' OVERWRITE INTO TABLE t_base_ec_shopitem_b PARTITION (ds='20161112');