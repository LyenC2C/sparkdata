# coding:utf-8
from pyspark import SparkContext


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

sc = SparkContext(appName="fri_mining")
# sq = sc.textFile("/hive/warehouse/wl_service.db/t_lel_shangqi_phoneid_20170405")\
#        .map(lambda a: (a.split("\t")[1],None)).collectAsMap()
#
# sq_map = sc.broadcast(sq)
# sq_v = sq_map.value
#
# def split(fields):
#     data = fields.split("\001")
#     id = data[0]
#     ids = data[1]
#     return (id, ids)
# fri = sc.textFile("/hive/warehouse/wl_base.db/t_base_weibo_user_fri/ds=20161106")\
#         .map(lambda a: split(a))
#
# #非上汽id
# def filter_sq_nonu_f(a, b):
#     return (a, b) if not sq_v.has_key(a) else None
# #上汽id
# def filter_sq_id(a, b):
#     return (a, b) if sq_v.has_key(a) else None
# #非上汽id中ids中有上汽
# def split_ids(v):
#     ids = v.split(",")
#     for i in ids:
#         sq_v.has_key(i)
#         return 1
#
# def split_id_ids(a,b):
#     ids = b.split(",")
#     lv = []
#     for i in ids:
#         sq_v.has_key(i)
#         lv.append((i,2))
#     return lv
#
#
# fri_id_nonsq = fri.map(lambda (a,b):filter_sq_nonu_f(a,b)).filter(lambda a:a is not None)
#
# fri_ids_sq =  fri_id_nonsq.mapValues(lambda ids:split_ids(ids))
#
# fri_id_ids_sq =  fri.map(lambda (a,b):split_id_ids(a,b)).filter(lambda a:a is not None).flatMap(lambda (a,b):split_id_ids(a,b))
#
# fri_ids_sq.union(fri_id_ids_sq).reduceByKey(lambda a,b:a+b).filter(lambda (a,b): b >= 3)


#test
test = {"2530759420":"5281230376"}
test_map = sc.broadcast(test)
test_v = test_map.value

sq = sc.textFile("/hive/warehouse/wl_service.db/t_lel_shangqi_phoneid_20170405")\
       .map(lambda a: (a.split("\t")[1],None)).collectAsMap()
sq_map = sc.broadcast(sq)
sq_v = sq_map.value

fri = sc.textFile("/hive/warehouse/wl_service.db/t_lel_weibo_test") \
    .map(lambda a: split(a)) \

def split(fields):
    data = fields.split("\001")
    id = data[0]
    ids = data[1]
    return (id, ids)
fri = sc.textFile("/hive/warehouse/wl_base.db/t_base_weibo_user_fri/ds=20161106") \
    .map(lambda a: split(a))

#非上汽id
def filter_sq_nonu_f(a, b):
    return (a, b) if not sq_v.has_key(a) else None
#上汽id
def filter_sq_id(a, b):
    return (a, b) if sq_v.has_key(a) else None
#非上汽id中ids中有上汽
def split_ids(a,b):
    ids = b.split(",")
    lv=[]
    for i in ids:
        if sq_v.has_key(i):
            lv.append((a,i))
    if len(lv)==0:
        return None
    else:
        return lv
def split_id_ids(a,b):
    ids = b.split(",")
    lv = []
    for i in ids:
        if sq_v.has_key(i):
            lv.append((i,a))
    if len(lv)==0:
        return None
    else:
        return lv
#非上汽id
fri_id_nonsq = fri.map(lambda (a,b):filter_sq_nonu_f(a,b)).filter(lambda a:a is not None)
#非上汽id,上汽ids
fri_ids_sq =  fri_id_nonsq.flatMap(lambda (a,b):split_ids(a,b)).filter(lambda a:a is not None)
#上汽id,ids
fri_id_ids_sq =  fri.map(lambda (a,b):filter_sq_id(a,b)).filter(lambda a:a is not None).flatMap(lambda (a,b):split_id_ids(a,b)).filter(lambda a:a is not None)

re = fri_ids_sq.union(fri_id_ids_sq).groupByKey().mapValues(list).filter(lambda (a,b):len(b)>=2).saveAsTextFile("/user/lel/results/fri_mining_5")






