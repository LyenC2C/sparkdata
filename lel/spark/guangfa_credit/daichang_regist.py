from pyspark import SparkContext

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")




sc = SparkContext(appName="guangfa_credit")
data = sc.textFile("/hive/warehouse/wl_base.db/t_base_credit_bank/ds=20170221/*")
data.map(lambda a:a.split("\001"))
