#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="k_korea_iteminfo")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content
def f(x):
    result = []
    ob_s = json.loads(valid_jsontxt(x))
    # key = valid_jsontxt(ob_s.get("search_info","{}").get("key","-"))
    pdlist = ob_s.get("pdlist",[])
    for ob in pdlist:
        item_id = ob.get("item_id","-")
        title = ob.get("title","-")
        key = valid_jsontxt(ob.get("search_info","{}").get("key","-"))
        cate_name = "-"
        brand_name = "-"
        if "润膏" in key:
            cate_name = "洗发水"
            brand_name = "润膏"
        if "雪花秀" in key:
            cate_name = "面部护理套装"
            brand_name = "雪花秀"
        if "MEDIHEAL" in key:
            cate_name = "面膜"
            brand_name = "MEDIHEAL"
        if "兰芝" in key:
            cate_name = "BB霜"
            brand_name = "兰芝"
        if "贵爱娘" in key:
            cate_name = "卫生巾"
            brand_name = "贵爱娘"
        if "White" in key:
            cate_name = "卫生巾"
            brand_name = "White"
        if "BOSOMI" in key:
            cate_name = "纸尿片"
            brand_name = "BOSOMI"
        if "好奇" in key:
            cate_name = "纸尿片"
            brand_name = "好奇"
        if "惠人" in key:
            cate_name = "榨汁机"
            brand_name = "惠人"
        if "福库" in key:
            cate_name = "电饭煲"
            brand_name = "福库"
        if "兰芝" in key:
            cate_name = "面部护理套装"
            brand_name = "兰芝"
        laiyuan = "yhd"
        price_zone = "-"
        price = ob.get("price","-")
        rateCounts = ob.get("comment_count","-").split()[0]
        item_count = "1"
        ts = "-"
        if cate_name == "卫生巾" or cate_name == "纸尿片":
            if "包" in title:
                # tt = title.decode("utf-8")
                i = title.find("包")
                if i >= 0:
                    i = i-1
                    item_count = ""
                    while(title[i].isdigit() and i > 0):
                        item_count = title[i] + item_count
                        if i > 0: i = i - 1
                        else: break
                if item_count == "": item_count = '1'
            elif "片*" in title or "p*" in title or "P*" in title or "片x" in title or "片X" in title:
                for ln in ["片*","p*","P*","片x","片X"]:
                    i = title.find(ln)
                    if i <= 0: continue
                    else:
                        i = i + len(ln)
                        if i >= len(title):continue
                        item_count = ""
                        while(title[i].isdigit()):
                            item_count = item_count + title[i]
                            if i < len(title) - 1: i += 1
                            else: break
                        if item_count == "": item_count = "1"
                        break
        if cate_name == "洗发水":
            if "瓶" in title or "支" in title:
                # tt = title.decode("utf-8")
                for ln in ["瓶","支"]:
                    i = title.find(ln)
                    if i < 0: continue
                    else:
                        i = i - 1
                        item_count = ""
                        while(title[i].isdigit()):
                            item_count = title[i] + item_count
                            if i > 0: i = i - 1
                            else: break
                        if item_count == "": item_count = '1'
                        break
            elif "*" in title or "X" in title or "x" in title:
                for ln in ["*","X","x"]:
                    i = title.find(ln)
                    if i <= 0: continue
                    else:
                        i = i + 1
                        if i >= len(title):continue
                        item_count = ""
                        while(title[i].isdigit()):
                            item_count = item_count + title[i]
                            if i < len(title) - 1: i += 1
                            else: break
                        if item_count == "": item_count = "1"
                        break
        if cate_name == "面膜":
            if "片" in title:
                # tt = title.decode("utf-8")
                i = title.find("片")
                if i >= 0:
                    i = i-1
                    item_count = ""
                    while(title[i].isdigit() and i > 0):
                        item_count = title[i] + item_count
                        if i > 0: i = i - 1
                        else: break
                if item_count == "": item_count = '-'
            else:
                item_count = "-"
        lv = []
        lv.append(item_id)
        lv.append(title)
        lv.append(cate_name)
        lv.append(laiyuan)
        lv.append(brand_name)
        lv.append(str(price))
        lv.append((price_zone))
        lv.append(rateCounts)
        lv.append(item_count)
        lv.append(ts)
        lv_r = "\001".join([str(valid_jsontxt(i)) for i in lv])
        result.append(lv_r)
    return result


s_yhd = "/commit/project/hanguo3/yhd.4.iteminfo"
rdd = sc.textFile(s_yhd).flatMap(lambda x: f(x)).filter(lambda x:x!=None)
rdd.saveAsTextFile('/user/wrt/temp/t_korea_iteminfo_yhd')
