# coding:utf-8
import sys, rapidjson, time
import rapidjson as json

import zlib
import base64
import time

def compress(data):
    compressed = zlib.compress(data)
    out = base64.b64encode(compressed)
    return out


def decompress(out):
    decode = base64.b64decode(out)
    data = zlib.decompress(decode)
    return data

'''
1453221603	16213026863	{"apiStack":{"subInfos":["虚拟商品无运费","月销 1269509笔","浙江杭州"],"skuModel":{"installmentEnable":"false"},"guaranteeInfo":{"afterGuarantees":[{"title":"正品保证"},{"title":"不支持7天退换"},{"title":"极速退款"}]},"itemInfoModel":{"priceUnits":[{"price":"19.96","name":"价格","display":"1"}],"totalSoldQuantity":"1269509","points":"0","quantityText":"库存99件","soldQuantityText":"月销 1269509笔","isMakeup":"false","quantity":"99"},"layoutData":{"replaceDataMap":{"SUPER_ACT_TIME":"false"}},"delivery":{"areaId":"510104","updateAreaApi":{"name":"mtop","value":"{\"API_NAME\":\"com.taobao.detail.getTmallDyn\",\"VERSION\":\"5.0\",\"exParams\":\"{\\\"id\\\":\\\"16213026863\\\"}\",\"itemNumId\":\"16213026863\",\"needEcode\":false,\"needLogin\":false,\"wua\":false}"},"success":"true","deliveryFees":["虚拟商品无运费"],"destination":"锦江区","getAreaApi":{"name":"http","value":"http://hws.m.taobao.com/cache/com.taobao.detail.getAllArea/1.0"}},"abTestInfo":{"SingleWebview":"true"},"itemControl":{"unitControl":{"limitMultipleCount":"1","buyText":"立即购买","cartText":"加入购物车","cartSupport":"false","buySupport":"true"},"success":"true"},"extras":{"queryParams":{}}},"skuModel":{"installmentEnable":false},"itemInfoModel":{"itemId":"16213026863","sku":false,"itemIcon":"http://gw.alicdn.com/tps/i1/T1AV5DFSRbXXbaUxfe-36-32.png","itemTypeLogo":"http://gw.alicdn.com/tps/i1/T1AV5DFSRbXXbaUxfe-36-32.png","picsPath":["http://img.alicdn.com/imgextra/i4/TB196XLGXXXXXXGXpXXXXXXXXXX_!!0-item_pic.jpg"],"title":"浙江移动 手机 话费充值 20元 快充直充 24小时自动充值即时到账","itemTypeName":"tmall","itemUrl":"http://a.m.tmall.com/i16213026863.htm","saleLine":"online","location":"浙江杭州","isMakeup":false,"favcount":"1548","categoryId":"150401","stuffStatus":"全新"},"rateInfo":{"rateCounts":"1066499"},"defDyn":{"delivery":{"deliveryFees":["卖家包邮"]},"itemControl":{"unitControl":{"limitMultipleCount":1,"cartSupport":true,"cartText":"加入购物车","buyText":"立即购买","buySupport":true,"submitText":"立即购买"},"buyUrl":"buildOrderVersion=3.0"},"itemInfoModel":{"quantityText":"99","isMakeup":false,"priceUnits":[{"price":"19.96","display":2}],"quantity":99}},"trackParams":{"categoryId":"150401","BC_type":"B"},"seller":{"evaluateInfo":[{"highGap":"30.87","score":"4.9 ","name":"描述相符","title":"描述相符"},{"highGap":"16.20","score":"4.9 ","name":"服务态度","title":"服务态度"},{"highGap":"18.07","score":"4.9 ","name":"物流服务","title":"发货速度"}],"creditLevel":"20","starts":"2012-06-14 15:45:53","fansCount":"67197","goodRatePercentage":"100.00%","shopId":"72370856","nick":"浙江移动官方旗舰店","fansCountText":"6.7万","weitaoId":"2083121027","userNumId":"908875136","shopTitle":"浙江移动官方旗舰店","actionUnits":[{"url":"http://shop.m.taobao.com/goods/index.htm?shop_id=72370856","track":"Button-AllItem","name":"全部宝贝","value":"173"},{"url":"http://h5.m.taobao.com/weapp/view_page.htm?page=shop/new_item_list&userId=908875136","track":"Button-NewItem","name":"上新","value":"15"},{"name":"关注人数","value":"6.7万"}],"type":"B","picUrl":"http://img.alicdn.com/imgextra//36/a4/T1QAb2Fg4bXXb1upjX.jpg"},"layoutData":{"replaceDataMap":{"ITEM_ID":"16213026863","SELLER_ID":"908875136","SHOP_ID":"72370856"}},"extras":{"defDyn":"{\"delivery\":{\"deliveryFees\":[\"卖家包邮\"]},\"itemControl\":{\"buyUrl\":\"buildOrderVersion=3.0\",\"unitControl\":{\"buySupport\":true,\"buyText\":\"立即购买\",\"cartSupport\":true,\"cartText\":\"加入购物车\",\"limitMultipleCount\":1,\"submitText\":\"立即购买\"}},\"itemInfoModel\":{\"isMakeup\":false,\"priceUnits\":[{\"display\":2,\"price\":\"19.96\"}],\"quantity\":99,\"quantityText\":\"99\"}}"},"props":[{"name":"面值","value":"20元"},{"name":"地区","value":"浙江"},{"name":"充值方式","value":"自动充值"}],"descInfo":{"h5DescUrl":"http://hws.m.taobao.com/cache/wdesc/5.0?id=16213026863&f=TB1.OocLXXXXXayXpXX8qtpFXlX","pcDescUrl":"http://hws.m.taobao.com/cache/wdesc/5.0?id=16213026863&f=TB1.OocLXXXXXayXpXX8qtpFXlX","fullDescUrl":"http://hws.m.taobao.com/cache/mtop.wdetail.getItemFullDesc/4.1/?data=%7B%22item_num_id%22%3A%2216213026863%22%7D","briefDescUrl":"http://hws.m.taobao.com/cache/mtop.wdetail.getItemDescx/4.1/?data=%7B%22item_num_id%22%3A%2216213026863%22%7D","h5DescUrl2":"http://h5.m.taobao.com/app/detail/desc.html?_isH5Des=true#!id=16213026863&type=1&f=TB1.OocLXXXXXayXpXX8qtpFXlX","showFullDetailDesc":"1"}}

'''

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res

def pro_compress_line(line):
    ls = line.strip().split("\t")
    #flag:0-无效,1-正常,2-下架
    flag = 0
    try:
        j = json.loads(valid_jsontxt(ls[2]))
        if j.has_key("ret") and "ERRCODE_QUERY_DETAIL_FAIL" in j["ret"]:
            flag= 0
        elif j["apiStack"]["itemControl"]["unitControl"].has_key("offShelfUrl") == False:
            flag = 1
        elif j["apiStack"]["itemControl"]["unitControl"].has_key("offShelfUrl") == True:
            flag= 2
        int(ls[1])
    except Exception,e:
        #print e,line.encode("utf-8")
        return None

    #return itemid,[ts,falg,data]
    #return [ls[1],[int(ls[0]),flag,compress(line.strip())]]
    if len(ls[1]) >= 5 and len(ls[1]) <= 14:
        try:
            return [ls[1],[int(ls[0]),flag,compress(line.strip().encode("utf-8"))]]
        except Exception,e:
            print e,line.encode("utf-8")
            return None

def gen_item_base(x,y):
    status_flag = 0
    status_ts = 0
    data_flag = 0
    data_ts = 0
    data = ""

    #[ts,flag,content]
    sortls = sorted(y,key=lambda x:x[0],reverse=True)
    if sortls[0][1] != 0:
        status_flag = sortls[0][1]
        status_ts = sortls[0][0]
        data_flag = status_flag
        data_ts = status_ts
        data = sortls[0][2]
    else:
        status_flag = 0
        status_ts = sortls[0][0]
        for each in sortls[1:]:
            if each[1] != 0:
                data_ts = each[0]
                data_flag = each[1]
                data = each[2]
                break
    #return [status_flag,status_ts,data_flag,data_ts,data]
    return [str(x),str(status_flag),str(status_ts),str(data_flag),str(data_ts),data]
    #return str(x)+'\001'+str(status_flag)+'\001'+str(status_ts)+'\001'+str(data_flag)+'\001'+str(data_ts)+'\001'+data

def gen_item_inc(x,y):
    #1:新进数据   2:库中数据
    #[status_flag,status_ts,data_flag,data_ts,data]
    dic = {1:None,2:None}
    for each in y:
        dic[each[0]] = each[1]

    #只有库中数据
    if dic[1] == None:
        [status_flag,status_ts,data_flag,data_ts,data] = dic[2]
    #只有新进数据
    elif dic[2] == None:
        [status_flag,status_ts,data_flag,data_ts,data] = dic[1]
    #两个都存在
    else:
        if int(dic[1][1]) < int(dic[2][1]):
            [status_flag,status_ts,data_flag,data_ts,data] = dic[2]
        else:
            status_flag = dic[1][0]
            status_ts = dic[1][1]
            if dic[1][2] == '0' and dic[2][2] == '1':
                [data_flag,data_ts,data] = dic[2][2:]
            else:
                [data_flag,data_ts,data] = dic[1][2:]

    return [str(x),str(status_flag),str(status_ts),str(data_flag),str(data_ts),data]
    #return str(x)+'\001'+str(status_flag)+'\001'+str(status_ts)+'\001'+str(data_flag)+'\001'+str(data_ts)+'\001'+data


if __name__ == "__main__":
    if sys.argv[1] == "local":
        key = ""
        tmp = []
        for line in sys.stdin:
            [itemid,ls] = pro_compress_line(line)
            tmp.append(ls)
        res = gen_item_base(key,tmp)
        print res


    if sys.argv[1] == '-genbase':
        from pyspark import SparkContext
        sc = SparkContext(appName="xzx_iteminfo_genbase")
        input_path = sys.argv[2]
        output_path = sys.argv[3]
        rdd1 = sc.textFile(input_path)
        rdd1.map(lambda x:pro_compress_line(x))\
                    .filter(lambda x:x!=None)\
                    .groupByKey()\
                    .mapValues(list)\
                    .map(lambda (x,y):gen_item_base(x,y))\
                    .map(lambda x:"\001".join(x))\
                    .saveAsTextFile(output_path)

        sc.stop()
    #[str(x),str(status_flag),str(status_ts),str(data_flag),str(data_ts),data]
    elif sys.argv[1] == '-geninc':
        from pyspark import SparkContext
        sc = SparkContext(appName="iteminfo_base_inc")
        input_base_path = sys.argv[3]
        input_data_path = sys.argv[2]
        output_path = sys.argv[4]
        rdd_data = sc.textFile(input_data_path)\
                .map(lambda x:pro_compress_line(x))\
                .filter(lambda x:x!=None)\
                .groupByKey()\
                .mapValues(list)\
                .map(lambda (x,y):gen_item_base(x,y))\
                .map(lambda x:[x[0],[1,x[1:]]])
        rdd_base = sc.textFile(input_base_path)\
                .map(lambda x:x.split("\001"))\
                .map(lambda x:[x[0],[2,x[1:]]])
        rdd_data.union(rdd_base)\
                .groupByKey()\
                .map(lambda (x,y):gen_item_inc(x,y))\
                .map(lambda x:"\001".join(x))\
                .coalesce(1600)\
                .saveAsTextFile(output_path)

        sc.stop()

    elif sys.argv[1] == '-extract_xzx':
        def f(x,dic):
            return True if dic.has_key(x.split("\001")[0]) else False

        from pyspark import SparkContext
        input_id_path = sys.argv[2]
        input_base_path = sys.argv[3]
        output_path = sys.argv[4]
        sc = SparkContext(appName="extract xzx iteminfo from base by "+input_id_path)

        id_dic = sc.broadcast(
                sc.textFile(input_id_path)\
                    .map(lambda x:(x,1))\
                    .collectAsMap()
                )

        sc.textFile(input_base_path)\
                .filter(lambda x:f(x,id_dic.value))\
                .saveAsTextFile(output_path)

        sc.stop()

