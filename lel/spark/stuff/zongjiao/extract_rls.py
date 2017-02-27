from pyspark import SparkContext


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


sc = SparkContext(appName="extact_relationships")
#(id,kw)
idRDD = sc.textFile("/hive/warehouse/wl_service.db/t_lel_weibo_zongjiao_ik_20170113/*").map(
    lambda a: (a.split('\001')[0], a.split('\001')[1]))
id_dict = idRDD.collectAsMap()
id_dict_b = sc.broadcast(id_dict)
id_dict_bv = id_dict_b.value
#(id,ids)
fri_idsRDD = sc.textFile("/hive/warehouse/wl_base.db/t_base_weibo_user_fri/ds=20161106/*").map(
    lambda a: (a.split('\001')[0], a.split('\001')[1]))

#
infoRDD = sc.textFile("/hive/warehouse/wl_base.db/t_base_weibo_user_new/ds=20161123/*").map(
    lambda a: (a.split('\001')[0], (a.split('\001')[5], a.split('\001')[7], a.split('\001')[13], a.split('\001')[22])))


def extract(ids, id_dict):
    res = []
    for id in id_dict.keys():
        if id in ids:
            res.append(id)
    return ','.join(res)
#(id,kw) join (id,ids) => (id,(kw,ids))
re = idRDD.join(fri_idsRDD).mapValues(lambda (kw,ids): (kw, extract(ids, id_dict_bv))).map(lambda (a,(b,c)): '\001'.join(valid_jsontxt(i) for i in [a,b,c])).saveAsTextFile("/user/lel/results/zongjiao20170224_idkwids_v1")


'''
#(id,kw) join (id,ids) => (id,(kw,ids)) join (id,(city,decription,gender,vertified)) => (id,((kw,ids),(city,description,gender,vertified)))
re = idRDD.join(fri_idsRDD).mapValues(lambda (kw,ids): (kw, extract(ids, id_dict_bv)))\
                        .join(infoRDD)\
                        .map(lambda (a, ((b, c), (d, e, f, g))): "\001".join([valid_jsontxt(i) for i in [a, b, c, d, e, f, g]]))\
                        .saveAsTextFile("/user/lel/results/zongjiao20170224")
'''



