from pyspark import SparkContext

sc = SparkContext(appName="xianyu_iteminfo")


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def getItemAndCate(s):
    data = s.split('\001')
    itemid = data[0]
    root_cate = data[5]
    return (itemid, root_cate)



def join(a, b, itemids):
    if itemids.has_key(a):
        return valid_jsontxt(a) + '\t' + valid_jsontxt(b)
    else:
        return ''


data = sc.textFile("/user/lel/datas/lossitemid.csv").filter(lambda a: "enc_mobile" not in a).map(
    lambda a: (a.split(',')[1], ""))

itemid_dict = sc.broadcast(data.collectAsMap())

itemids = itemid_dict.value

iteminfo = sc.textFile("/hive/warehouse/wl_base.db/t_base_ec_item_dev_new/ds=20161224").map(
    lambda a: getItemAndCate(a)).filter(lambda a: a != None)

iteminfo.map(lambda (a, b): join(a, b, itemids)).filter(
    lambda a: a != '').saveAsTextFile("/user/lel/itemcateloss")
