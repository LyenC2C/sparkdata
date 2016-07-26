#coding:utf-8
__author__ = 'wrt'

import sys
import rapidjson as json
from pyspark import SparkContext
sc = SparkContext(appName="t_base_shopitem")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f1(line):
    ss = line.strip().split("\t",3)
    shop_id = ss[1]
    ts = ss[0]
    text = line.strip()
    star = text.find("({")
    if star == -1: return [None]
    else: star += 1
    end = text.rfind("})") + 1
    ob = json.loads(valid_jsontxt(text[star:end]))
    shopTitle = ob.get("data",{}).get("shopTitle","-")
    item_count = ob.get("data",{}).get("totalResults","-")
    itemsArray = ob.get("data",{}).get("itemsArray",[])
    result = []
    for item in itemsArray:
        lv = []
        auctionId = item.get("auctionId","-")
        title = item.get("title","-")
        picUrl = item.get("picUrl","-")
        sold = item.get("sold","-")
        day_sold = '0' #日销量，默认为0
        reservePrice = item.get("reservePrice","-")
        salePrice = item.get("salePrice","-")
        auctionType = item.get("auctionType","-")
        quantity = item.get("quantity","-")
        totalSoldQuantity = item.get("totalSoldQuantity","-")
        orderCost = item.get("orderCost","-")
        bonusAmount = item.get("bonusAmount","-")
        onSale = item.get("onSale","-")
        up_day = today #默认为今日新上架，后面会进行调整
        down_day = "0"
        lv.append(valid_jsontxt(shop_id))
        lv.append(valid_jsontxt(shopTitle))
        lv.append(valid_jsontxt(item_count))
        lv.append(valid_jsontxt(auctionId))
        lv.append(valid_jsontxt(title))
        lv.append(valid_jsontxt(picUrl))
        lv.append(valid_jsontxt(sold))
        lv.append(valid_jsontxt(day_sold))
        lv.append(valid_jsontxt(reservePrice))
        lv.append(valid_jsontxt(salePrice))
        lv.append(valid_jsontxt(auctionType))
        lv.append(valid_jsontxt(quantity))
        lv.append(valid_jsontxt(totalSoldQuantity))
        lv.append(valid_jsontxt(orderCost))
        lv.append(valid_jsontxt(bonusAmount))
        lv.append(valid_jsontxt(onSale))
        lv.append(valid_jsontxt(up_day)) #上架日期
        lv.append(valid_jsontxt(down_day)) #0代表上架，日期代表下架日期
        lv.append(valid_jsontxt(ts))
        result.append((auctionId,lv))
    return result

def f2(line):
    ss = line.strip().split('\001')
    return ss

def quchong(x,y):
    max = 0
    item_list = y
    for ln in item_list:
        if int(ln[-1]) > max:
            max = int(ln[-1])
            y = ln
    return y

def twodays(x,y):   #同一个item_id下进行groupby后的结果
    item_list = y
    if len(item_list) == 1: #只有一个商品
        if len(item_list[0]) == 20:
            yes_item = item_list[0] #此商品为昨日商品（昨日商品多个ds字段），今日商品需要复制昨日商品
            if yes_item[17] == '0': #此昨日商品在今天之前没下架，但是今天下架了;否则是今天之前就下架了，那么复制即可
                yes_item[17] = today #设置down_day为今日日期
            yes_item[7] = '0' #已下架商品日销量为0
            result = yes_item[:-1]   #记得将最后的ds去掉，不要复制进来
        if len(item_list[0]) == 19:
            tod_item = item_list[0] #此商品为今日商品，说明此商品今天上架，此前没出现过
            result = tod_item #使用默认值即可
    if len(item_list) == 2: #有两个商品，一个是昨日，一个时间今日
        #判断今日和昨日的位置并分别命名赋值
        if len(item_list[0]) == 20:
            yes_item = item_list[0]
            tod_item = item_list[1]
        if len(item_list[0]) == 19:
            tod_item = item_list[0]
            yes_item = item_list[1]
        tod_item[16] = yes_item[16] #无论此商品在曾经是否下过架，今天都已经上架了，那么复制他的上架时间即可
        tod_item[7] = str(int(tod_item[6]) - int(yes_item[6])) #计算日销量，今日减昨日
        result = tod_item
    return "\001".join(result)


today = sys.argv[1]
yesterday = sys.argv[2]


s1 = "/commit/shopitem/20160726/192*"
s2 = "/hive/warehouse/wlbase_dev.db/t_base_ec_shopitem_dev/ds=" + yesterday

# rdd1_c = sc.textFile(s1).flatMap(lambda x:f1(x)).filter(lambda x:x != None)
# rdd1 = rdd1_c.groupByKey().mapValues(list).map(lambda (x, y):quchong(x, y))
rdd2 = sc.textFile(s2).map(lambda x:f2(x)).filter(lambda x:x != None)
# rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x, y):twodays(x, y))
rdd1.saveAsTextFile('/user/wrt/shopitem_tmp')
#hfs -rmr /user/wrt/shopitem_tmp
#spark-submit  --executor-memory 9G  --driver-memory 8G  --total-executor-cores 120 t_base_shopitem.py 20160722 20160721
#LOAD DATA  INPATH '/user/wrt/shopitem_tmp' OVERWRITE INTO TABLE t_base_ec_shopitem_dev PARTITION (ds='20160722');