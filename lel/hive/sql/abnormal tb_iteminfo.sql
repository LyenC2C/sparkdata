abnormal tb_iteminfo:

js = sc.textFile("/commit/iteminfo/tb_iteminfo/177.tb.iteminfo.20170418").map(lambda a: valid_jsontxt(a.split("\t")[2]))

import rapidjson as json
js_ob = js.map(lambda a: json.loads(a))


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

df = sqlContext.jsonRDD(js_ob)



dataframe_practice:

data = sqlContext.read.json("/user/lel/temp/test").cache()
data.registerTempTable("data")
df = sqlContext.sql(
"select itemInfoModel.location as location,itemInfoModel.title, from data where itemInfoModel is not null")


