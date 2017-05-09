from pyspark import SparkContext


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


sc = SparkContext(appName="fri_mining")

# product

wbid = sc.textFile("/trans/lel/shen/shen_other_channel.teltbsnwb") \
    .map(lambda a: a.split("\001")[2]).filter(lambda a: "None" not in a).map(lambda a: (a, None)).collectAsMap()
wbid_mp = sc.broadcast(wbid)
wbid_v = wbid_mp.value


def splitAndFilter(fields):
    data = fields.split("\001")
    id = data[0]
    ids = data[1]
    return (id, ids) if wbid_v.has_key(id) else None

#肾->微博 (id,ids) -> (id,id)
fri = sc.textFile("/hive/warehouse/wl_base.db/t_base_weibo_user_fri/ds=20161106") \
    .map(lambda a: splitAndFilter(a)).filter(lambda a: a is not None).flatMapValues(lambda a: a.split(",")).map(lambda (a,b):(b,a))


arr_id = ["1933922642","1403418835","1794814505","1069266662","2309515393","1919440540","3172690077","3142304435","1648792581","2510278004","5983104188","2951947341","2312196485","2141644814","2692697804","2013702115","2485584101","2147045512","5116569680","1498651723","1927013245","1340438733","2186480255","2023983335","5578063472","1547578905","2560024420","2703082040","2606939053","2178994292","5578063472","1547578905","2560024420","2703082040","2606939053","2178994292","2254645210","5912831472","1857108075","2523720952"]

big_vs_dict = {}
for v in arr_id:
    big_vs_dict.setdefault(valid_jsontxt(v))
big_vs_dict_v= sc.broadcast(big_vs_dict).value

def split(str):
    data = valid_jsontxt(str).split("\001")
    if len(data) <= 22: return None
    id = data[0]
    name = data[3]
    verified = data[22]
    if "True" in verified and big_vs_dict_v.has_key(id):
        return (id, name)
    else:
        return None

#微博大V ->(id,name)
new = sc.textFile("/hive/warehouse/wl_base.db/t_base_weibo_user_new/ds=20161123").map(lambda a: split(a)).filter(lambda a:a is not None)

fri.join(new).map(lambda (a,(b,c)): (b,c)).reduceByKey(lambda a,b: a+ "|" +b).map(lambda (a,b):a + "\001" + b).saveAsTextFile("/user/lel/results/shen")
