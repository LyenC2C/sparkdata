from pyspark import SparkContext

'''
product to pzz:

给定V weibo_id,得到V所对应的粉丝列表,返回weibo_id\001fans_id
'''


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


v = ['1805286132', '1746664450', '1254433995', '1239246050', '1624923463', '1717629720', '1909802154', '2689280541',
     '1688351602', '2257585590']


def getfans(content):
    fields = content.split('\001')
    id = valid_jsontxt(fields[0])
    ids = valid_jsontxt(fields[1])
    result = []
    for i in v:
        if i in ids:
            result.append((id, i))
    return result


sc = SparkContext(appName="xianyu_iteminfo")
data = sc.textFile("/hive/warehouse/wl_base.db/t_base_weibo_user_fri/ds=20161106")
data.flatMap(lambda a: getfans(a))\
    .map(lambda a: (a[1], a[0])).groupByKey().mapValues(list)\
    .map(lambda a: a[0] + '\0' + ','.join(a[1]))\
    .coalesce(1)\
    .saveAsTextFile('/user/lel/weibo')
