find_fri:

data = sqlContext.sql("select id from wl_analysis.t_lt_weibo_fraud_usr")
rist_mapid = data.map(lambda a:(str(a.id),None)).collectAsMap()
rist_mapid_b = sc.broadcast(rist_mapid)
rist_mapid_v = rist_mapid_b.value

def filter_both(a, b):
    return (a, b) if rist_mapid_v.has_key(a) and rist_mapid_v.has_key(b) else None

data_fri = sqlContext.sql("select id,ids from wl_base.t_base_weibo_user_fri where ds = '20161106'")
fri_rddFlated = data_fri.map(lambda a: (str(a.id),a.ids)).flatMapValues(lambda a:a.split(",")).map(lambda (a,b):filter_both(a,b)).filter(lambda a: a is not None).map(lambda (a,b):((a,b),None))
fri_rddFlated_e = fri_rddFlated.map(lambda ((a,b),c):((b,a),c))
re = fri_rddFlated.union(fri_rddFlated_e).groupByKey().filter(lambda ((a,b),c):len(c) == 2).map(lambda ((a,b),c): a + "\001" + b).saveAsTextFile("/user/lel/results/risk")

######################
test_focus_eachother:#
######################
data_fri = sqlContext.sql("select id,regexp_extract(ids,'2530759420|5281230376',0) as ids from wl_base.t_base_weibo_user_fri where ds = '20161106' and id regexp '2530759420|5281230376' and ids regexp '2530759420|5281230376'").cache()

fri_rddFlated = data_fri.map(lambda a: ((str(a.id),a.ids),None))
fri_rddFlated_e = fri_rddFlated.map(lambda ((a,b),c):((b,a),c))
re = fri_rddFlated.union(fri_rddFlated_e).groupByKey().filter(lambda ((a,b),c):len(c) == 2).map(lambda ((a,b),c): a + "\001" + b).saveAsTextFile("/user/lel/results/risk")

#################
stat            # 
#################
wl_service.t_lel_weibo_abnomal_fri

wl_analysis.t_lt_weibo_fraud_usr



