__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext


sc = SparkContext(appName="jiexi_wine")
def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content
def f(line):
    ob = json.loads(valid_jsontxt(line.strip()))
    skuid = ob["skuid"]
    category1 = ob["category1"]
    category2 = ob["category2"]
    category3 = ob["category3"]
    name = ob["name"]
    weight = ob["weight"]
    title = ob["title"]
    xiangxin = ob["xiangxin"]
    shengfeng = ob["shengfeng"]
    brand = ob ["brand"]
    product_place = ob["product_place"]
    dushu = ob["dushu"]
    guige = ob["guige"]
    lv = []
    lv.append(valid_jsontxt(str(skuid)))
    lv.append(valid_jsontxt(str(category1)))
    lv.append(valid_jsontxt(str(category2)))
    lv.append(valid_jsontxt(str(category3)))
    lv.append(valid_jsontxt(str(name)))
    lv.append(valid_jsontxt(str(weight)))
    lv.append(valid_jsontxt(str(title)))
    lv.append(valid_jsontxt(str(xiangxin)))
    lv.append(valid_jsontxt(str(shengfeng)))
    lv.append(valid_jsontxt(str(brand)))
    lv.append(valid_jsontxt(str(product_place)))
    lv.append(valid_jsontxt(str(dushu)))
    lv.append(valid_jsontxt(str(guige)))
    return "\001".join(lv)
rdd = sc.textFile("/user/zlj/temp/jindong_jiu.json").map(lambda x:f(x))
rdd.saveAsTextFile('/user/wrt/jingdong_jiu')