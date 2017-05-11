# coding:utf-8
from pyspark import SparkContext

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

sc = SparkContext(appName="fri_mining")

#product

sq = sc.textFile("/hive/warehouse/wl_service.db/t_lel_shangqi_phoneid_20170405")\
       .map(lambda a: (a.split("\t")[1],None)).collectAsMap()
sq_map = sc.broadcast(sq)
sq_v = sq_map.value

def split(fields):
    data = fields.split("\001")
    id = data[0]
    ids = data[1]
    return (id, ids)
fri = sc.textFile("/hive/warehouse/wl_base.db/t_base_weibo_user_fri/ds=20161106") \
    .map(lambda a: split(a)).flatMapValues(lambda a:a.split(","))

#非上汽id,上汽ids
def filter_sq_nonu_f(a, b):
    return (a, b) if not sq_v.has_key(a) and sq_v.has_key(b) else None

#上汽id,非上汽ids
def filter_sq_id(a, b):
    return (a, b) if sq_v.has_key(a) and not sq_v.has_key(b) else None

#上汽id,上汽ids
def filter_sq_both(a, b):
    return (a, b) if sq_v.has_key(a) and sq_v.has_key(b) else None

#非上汽id,上汽ids
fri_ids_sq =  fri.map(lambda (a,b):filter_sq_nonu_f(a,b)) \
    .filter(lambda a:a is not None)

#上汽id,非上汽ids
fri_id_ids_sq =  fri.map(lambda (a,b):filter_sq_id(a,b)).filter(lambda a:a is not None).map(lambda (a,b):(b,a))

#上汽id,上汽ids
fri_sq_both =  fri.map(lambda (a,b):filter_sq_both(a,b)).filter(lambda a:a is not None).map(lambda (a,b):(b,a))
fri_sq_both_reverse =  fri_sq_both.map(lambda (a,b):(b,a))
#与用户1相互关注的非上汽用户
#way_1:明确知道valuelist的length最大为2
re = fri_ids_sq.union(fri_id_ids_sq) \
    .map(lambda a:(a,None)) \
    .reduceByKey(lambda a, b: None if len([a, b]) != 2 else [a, b]) \
    .filter(lambda (a,b): b is not None) \
    .map(lambda (a,b):(a[0],a[1])) \
    .filter(lambda a: a is not None)\
    .map(lambda (a,b): a + "\001" +b)\
    .saveAsTextFile("/user/lel/results/fri_mining_ids_sq")
#way_2:
re = fri_ids_sq.union(fri_id_ids_sq) \
    .map(lambda a:(a,None)) \
    .groupByKey().mapValues(list)\
    .filter(lambda (a,b): len(b) == 2) \
    .map(lambda (a,b):(a[0],a[1])) \
    .map(lambda (a,b): a + "\001" +b) \
    .saveAsTextFile("/user/lel/results/fri_mining_ids_sq")
#way_3:combineByKey代替groupByKey

#上汽用户中与其相互关注的好友（用户2）
#ps:仍然要注意以上方法中特定的reduceByKey的实现方式
re = fri_sq_both.union(fri_sq_both_reverse) \
    .map(lambda a:(a,None)) \
    .reduceByKey(lambda a, b: None if len([a, b]) != 2 else [a, b]) \
    .filter(lambda (a,b): b is not None) \
    .map(lambda (a,b):(a[0],a[1])) \
    .filter(lambda a: a is not None) \
    .map(lambda (a,b): a + "\001" +b) \
    .saveAsTextFile("/user/lel/results/fri_mining_both_sq")


'''
#test
test = {"2530759420":"5281230376","5281230376":"2530759420"}
test_map = sc.broadcast(test)
test_v = test_map.value

fri = sc.textFile("/hive/warehouse/wl_service.db/t_lel_weibo_test") \
    .map(lambda a: split(a)).flatMapValues(lambda a:a.split(","))

def split(fields):
    data = fields.split("\001")
    id = data[0]
    ids = data[1]
    return (id, ids)

#非上汽id,上汽ids
def filter_sq_nonu_f(a, b):
    return (a, b) if not test_v.has_key(a) and test_v.has_key(b) else None

#上汽id,非上汽ids
def filter_sq_id(a, b):
    return (a, b) if test_v.has_key(a) and not test_v.has_key(b) else None

def both_sq(a, b):
    return (a, b) if test_v.has_key(a) and test_v.has_key(b) else None



#非上汽id,上汽ids
fri_ids_sq =  fri.map(lambda (a,b):filter_sq_nonu_f(a,b))\
              .filter(lambda a:a is not None)

#上汽id,非上汽ids
fri_id_ids_sq =  fri.map(lambda (a,b):filter_sq_id(a,b)).filter(lambda a:a is not None).map(lambda (a,b):(b,a))

re = fri_ids_sq.union(fri_id_ids_sq)\
    .map(lambda a:(a,None)) \
    .reduceByKey(lambda a, b: None if len([a, b]) != 2 else [a, b]) \
    .filter(lambda (a,b): b is not None) \
    .map(lambda (a,b):(a[0],a[1])) \
    .filter(lambda a: a is not None)\

#both
sq_both =  fri.map(lambda (a,b):both_sq(a,b)).filter(lambda a:a is not None).map(lambda (a,b):(b,a))
sq_both_reverse =  sq_both.map(lambda (a,b):(b,a))

test_both = sq_both.union(sq_both_reverse) \
    .map(lambda a:(a,None)) \
    .reduceByKey(lambda a, b: None if len([a, b]) != 2 else [a, b]) \
    .filter(lambda (a,b): b is not None) \
    .map(lambda (a,b):(a[0],a[1])) \
    .filter(lambda a: a is not None) \
    .map(lambda (a,b): a + "\001" +b) \
    .saveAsTextFile("/user/lel/results/fri_mining_both_sq")
'''





