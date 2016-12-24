#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext(appName="rong360_features_hebing")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")


def feature_main(line):
    ss = line.strip().split("\001")
    tel = ss[0]
    result = []
    i = -1
    for ln in ss[1:]:
        i += 1
        if not ln.replace(".","").isdigit(): continue
        # if valid_jsontxt(ln) == '\\N': continue #null数据当成0，直接continue
        # if ln.replace(".","").isdigit():
        if float(ln) == 0.0: continue
        ln == round(float(ln),2)
        result.append(valid_jsontxt(i) + ":" + valid_jsontxt(ln))
    return (tel,result)

def f_cat_reindex(line):
    ss = line.strip().split("\001")
    features = ss[1].split(" ")
    return [(valid_jsontxt(i.split(':')[0]),"") for i in features]

def feature_cat(line,fea_cat_dict,len_main):
    ss = line.strip().split("\001")
    tel = ss[0]
    features = ss[1].split(" ")
    result = []
    sort_dict = {} #用来排序
    for feature in features:
        ff = feature.split(":")
        f_value = ff[0]
        v_value = ff[1]
        if float(v_value) == 0.0: continue
        f_index = fea_cat_dict[f_value] + len_main
        sort_dict[f_index] = round(float(v_value),2)
        # result.append(f_index + ":" + valid_jsontxt(v_value))
    temp = sorted(sort_dict.iteritems(), key=lambda d:d[0], reverse = False)
    for ln in temp:
        result.append(valid_jsontxt(ln[0]) + ":" + valid_jsontxt(ln[1]))
    return (tel,result)

def tran_dict(fea_cat):
    fea_cat_dict = {}
    for i in range(len(fea_cat)):
        fea_cat_dict[fea_cat[i]] = i
    return fea_cat_dict

def fea_index(fea_all):
    fea_all_index = []
    for i in range(len(fea_all)):
        fea_all_index.append(valid_jsontxt(fea_all[i]) + "\t" + str(i))
    return fea_all_index

def hebing(x,y):
    result =[x] + y[0] + y[1]
    return " ".join([valid_jsontxt(ln) for ln in result])

def inhive(line):
    ss = line.strip().split(" ",1)
    return "\001".join(ss)


s_cat = "/hive/warehouse/wlcredit.db/t_wrt_credit_record_cate_feature/*" #稀疏
s_main = "/hive/warehouse/wlcredit.db/t_credit_dense_features/*" #紧密

#紧密特征字段提取并自动排列好
rdd_fea_main = hiveContext.sql('desc wlcredit.t_credit_dense_features')
fea_main = [valid_jsontxt(ln.col_name) for ln in rdd_fea_main.collect()[1:]]
# fea_main = ['buycnt','weibo_followers_count']
#记录紧密特征长度，后面的在分配feature_cat的index时候用到
len_main = len(fea_main)
#稀疏特征字段去重并按照一定顺序排列好
rdd_fea_cat = sc.textFile(s_cat).flatMap(lambda x:f_cat_reindex(x)).groupByKey().mapValues(list).map(lambda (x, y): x)
fea_cat = rdd_fea_cat.collect()
#将稀疏特征字段变成map形式，以提高效率
fea_cat_dict = tran_dict(fea_cat)
#每个电话按照新的稀疏特征顺序，将每个稀疏特征值依次输出，之前没有的特征用0代替
rdd_cat = sc.textFile(s_cat).map(lambda x:feature_cat(x,fea_cat_dict,len_main))
# rdd_m = hiveContext.sql('select tel_index,buycnt,weibo_followers_count from wlbase_dev.t_base_user_profile_telindex')
# rdd_main = rdd_m.map(lambda x:valid_jsontxt(x.tel_index) + "\001" + valid_jsontxt(x.buycnt) + "\001" \
#                                             + valid_jsontxt(x.weibo_followers_count)).map(lambda x:feature_main(x))
#每个电话按照紧密特征顺序，将每个一级特征值依次输出,0的过滤
rdd_main = sc.textFile(s_main).map(lambda x:feature_main(x))
#两个特征集合进行join操作，最终输出一个电话对应所有特征，特征按照先紧密特征，后稀疏特征的顺序
rdd = rdd_main.join(rdd_cat).map(lambda (x,y):hebing(x,y))
rdd.saveAsTextFile('/user/wrt/temp/all_feature_main_cat')
rdd_table = rdd.map(lambda x:inhive(x))
rdd_table.saveAsTextFile('/user/wrt/temp/all_feature_dense_sparse_inhive')
#全部特征字段输出.
fea_all = fea_main + fea_cat
fea_all_index = fea_index(fea_all)
sc.parallelize(fea_all_index).saveAsTextFile('/user/wrt/temp/all_features_name')
# hiveContext.sql('drop table wlcredit.t_wrt_credit_all_features_name')
hiveContext.sql('load data inpath "/user/wrt/temp/all_features_name" overwrite into table wlcredit.t_wrt_credit_all_features_name_20161223')
# create table wlcredit.t_wrt_credit_all_features_name_20161223 (feature string,index string)
# ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'   LINES TERMINATED BY '\n'
# stored as textfile

# hfs -rmr /user/wrt/temp/all_feature_main_cat && hfs -rmr /user/wrt/temp/all_features_name && hfs -rmr /user/wrt/temp/all_feature_dense_sparse_inhive
# spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 feature_dense_sparse.py
#此程序产出一份

 # LOAD DATA     INPATH '/user/wrt/temp/all_feature_dense_sparse_inhive' OVERWRITE
  # INTO TABLE wlcredit.t_credit_feature_merge PARTITION (ds = '20161223');