#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
from pyspark.sql import *

today = sys.argv[1]


sc = SparkContext(appName="rong360_features_hebing")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")


def feature_main(line,feature5k_dict,fea_main):
    ss = line.strip().split("\001")
    tel = ss[0]
    # result = []
    main_dict = {}
    i = -1
    for ln in ss[1:]:
        i += 1
        if not feature5k_dict.has_key(fea_main[i]): continue
        f_index = feature5k_dict[fea_main[i]]
        if not ln.replace(".","").isdigit(): continue
        if float(ln) == 0.0: continue
        ln = round(float(ln),2)
        main_dict[valid_jsontxt(f_index)] = valid_jsontxt(ln)
        # result.append(valid_jsontxt(index) + ":" + valid_jsontxt(ln))
    return (tel,main_dict)

def f_cat_reindex(line):
    ss = line.strip().split("\001")
    features = ss[1].split()
    return [(valid_jsontxt(i.split(':')[0]),"") for i in features]

def feature_cat(line,feature5k_dict):
    ss = line.strip().split("\001")
    tel = ss[0]
    features = ss[1].split()
    result = []
    # sort_dict = {} #用来排序
    cat_dict = {}
    for feature in features:
        ff = feature.split(":")
        f_value = valid_jsontxt(ff[0])
        v_value = valid_jsontxt(ff[1])
        if float(v_value) == 0.0: continue
        if not feature5k_dict.has_key(f_value): continue
        f_index = feature5k_dict[f_value]
        # sort_dict[f_index] = round(float(v_value),2)
        cat_dict[valid_jsontxt(f_index)] = valid_jsontxt(round(float(v_value),2))
        # result.append(valid_jsontxt(f_index) + ":" + valid_jsontxt(v_value))
    # temp = sorted(sort_dict.iteritems(), key=lambda d:d[0], reverse = False)
    # for ln in temp:
    #     result.append(valid_jsontxt(ln[0]) + ":" + valid_jsontxt(ln[1]))
    return (tel,cat_dict)
    # return (tel,result)

def tran_dict(f_list):
    f_list_dict = {}
    for i in range(len(f_list)):
        f_list_dict[valid_jsontxt(f_list[i])] = i
    return f_list_dict

def fea_index(fea_all):
    fea_all_index = []
    for i in range(len(fea_all)):
        fea_all_index.append(valid_jsontxt(fea_all[i]) + "\t" + str(i))
    return fea_all_index


def hebing(x,y):
    # result =[x] + y[0] + y[1]
    f_dict = dict(y[0], **y[1])
    temp = sorted(f_dict.iteritems(), key=lambda d:d[0], reverse = False)
    result = []
    for ln in temp:
        result.append(valid_jsontxt(ln[0]) + ":" + valid_jsontxt(ln[1]))
    return valid_jsontxt(x) + '\001' +" ".join(result)

def inhive(line):
    ss = line.strip().split(" ",1)
    return "\001".join(ss)

# def index_5k(x):
#     for i in range()


s_cat = "/hive/warehouse/wlcredit.db/t_wrt_credit_record_cate_feature_online/*" #稀疏
# s_cat = "/hive/warehouse/wlcredit.db/t_wrt_credit_record_cate_feature_online/000000_0"
s_main = "/hive/warehouse/wlcredit.db/t_credit_dense_features_online/*" #紧密
# s_main = "/hive/warehouse/wlcredit.db/t_credit_dense_features_online/000000_0"




#紧密特征字段提取并自动排列好
rdd_fea_main = hiveContext.sql('desc wlcredit.t_credit_dense_features_online')
fea_main = [valid_jsontxt(ln.col_name) for ln in rdd_fea_main.collect()[1:]]
# 线上指定特征导入并存成数组
feature5k = sc.textFile("/user/wrt/feature_5k").collect()
# 将数组变成字典并输出存表
feature5k_dict = tran_dict(feature5k)
fea_all_index = fea_index(feature5k)
sc.parallelize(fea_all_index).saveAsTextFile('/user/wrt/temp/all_features_name_v2')
# cat的rdd
rdd_cat = sc.textFile(s_cat).map(lambda x:feature_cat(x,feature5k_dict))
# rdd_m = hiveContext.sql('select tel_index,buycnt,weibo_followers_count from wlbase_dev.t_base_user_profile_telindex')
# rdd_main = rdd_m.map(lambda x:valid_jsontxt(x.tel_index) + "\001" + valid_jsontxt(x.buycnt) + "\001" \
#                                             + valid_jsontxt(x.weibo_followers_count)).map(lambda x:feature_main(x))
#每个电话按照紧密特征顺序，将每个一级特征值依次输出,0的过滤
rdd_main = sc.textFile(s_main).map(lambda x:feature_main(x,feature5k_dict,fea_main))
#两个特征集合进行join操作，最终输出一个电话对应所有特征，特征按照先紧密特征，后稀疏特征的顺序
rdd_table = rdd_main.join(rdd_cat).map  (lambda (x,y):hebing(x,y))
# rdd.saveAsTextFile('/user/wrt/temp/all_feature_main_cat_v2') #存libsvm格式
# rdd_table = rdd.map(lambda x:inhive(x))
rdd_table.saveAsTextFile('/user/wrt/temp/all_feature_dense_sparse_inhive_v2')

# hiveContextk.sql('drop table wlcredit.t_wrt_credit_all_features_name')
hiveContext.sql('load data inpath "/user/wrt/temp/all_features_name_v2" overwrite into table \
wlcredit.t_wrt_credit_all_features_name PARTITION (ds ='+ today +')')
hiveContext.sql('LOAD DATA INPATH "/user/wrt/temp/all_feature_dense_sparse_inhive_v2" OVERWRITE \
INTO TABLE wlcredit.t_credit_feature_merge PARTITION (ds = '+ today +')')
# create table wlcredit.t_wrt_credit_all_features_name (feature string,index string)
# PARTITIONED BY  (ds STRING )
# ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'   LINES TERMINATED BY '\n'
# stored as textfile

# hfs -rmr /user/wrt/temp/all_feature_main_cat && hfs -rmr /user/wrt/temp/all_features_name && hfs -rmr /user/wrt/temp/all_feature_dense_sparse_inhive
# spark-submit --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 feature_dense_sparse_onlin_v2.py
# 此程序产出一份
#
# LOAD DATA     INPATH '/user/wrt/temp/all_feature_dense_sparse_inhive' OVERWRITE
# INTO TABLE wlcredit.t_credit_feature_merge PARTITION (ds = '20161226');