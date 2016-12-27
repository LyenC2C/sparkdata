from pyspark import SparkContext

sc = SparkContext(appName="xianyu_iteminfo")


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


data = sc.textFile("/user/lel/datas/lossitemid.csv").filter(lambda a: "enc_mobile" not in a).map(
    lambda a: a.split(',')[1])

itemid = sc.broadcast(data.collect())


def getItemAndCate(s):
    data = s.split('\001')
    itemid = data[0]
    root_cate = data[5]
    if '-' not in root_cate:
        if '/' in root_cate:
            top_cate = root_cate.split('/')[0]
        else:
            top_cate = root_cate
    else:
        return None
    return (itemid, top_cate)


itemids = itemid.value


def re(a, b,itemids):
    if a in itemids:
        return valid_jsontxt(a) + '\001' + valid_jsontxt(b)
    else:
        return ''


iteminfo = sc.textFile("/hive/warehouse/wl_base.db/t_base_ec_item_dev_new/ds=20161224").map(
    lambda a: getItemAndCate(a)).filter(lambda a: a != None)

iteminfo.map(lambda (a, b): re(a, b,itemids)).filter(
    lambda a: a != '').saveAsTextFile("/user/lel/loss")

