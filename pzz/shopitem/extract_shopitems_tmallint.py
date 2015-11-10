#coding:utf-8

import rapidjson as json
import sys

#店铺id 商品id 商品title 总销量 售价 原价 picurl

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\001", "").replace("\n", " ")

def format(line):
    j = json.loads(line.strip())
    ls = [j["shopId"]]
    for item in j["data"]["itemsArray"]:
        ls.append(item["auctionId"])
        ls.append(item["title"])
        ls.append(item["totalSoldQuantity"])
        ls.append(item["salePrice"])
        ls.append(item["reservePrice"])
        ls.append(item["picUrl"])
    return "\t".join(ls)

if __name__ == '__main__':
    for line in sys.stdin:
        print format(line.strip())