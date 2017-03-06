#coding:utf-8
__author__ = 'wrt'
#此脚本目的主要是全量特征基础上加入一些占比特征,跑芝麻信用的数据
import sys
import rapidjson as json
import copy
from pyspark import SparkContext
from pyspark.sql import *

today = sys.argv[1]
# today_name = sys.argv[2]

sc = SparkContext(appName="add_new_feat")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

# def f(line):

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def add_index(feature_raw,feature_raw_len):
    numerator_price = {}
    numerator_count = {}
    newfeature = copy.copy(feature_raw)
    # add_list.append("1") #monthall_buy_count
    # add_list.append("2") #monthall_price_sum
    # add_index = 363734 #特征数量,select count(1) from t_wrt_credit_all_features_name where ds = '' 可得出
    # add_index += 1 #特征index是从1开始,所以add_index是要+1的
    for i in range(2,len(feature_raw)):
        ln = valid_jsontxt(feature_raw[i])
        if ("sum_price_level" in ln) or ("price_sum" in ln):
            numerator_price[str(i + 1)] = str(feature_raw_len) #+1依然是因为从1开始的原因,
            ln += "_ratio"
            newfeature.append(ln)
            feature_raw_len += 1
        elif ("count_level" in ln) or ("buy_count" in ln):
            numerator_count[str(i + 1)] = str(feature_raw_len)
            ln += "_ratio"
            newfeature.append(ln)
            feature_raw_len += 1
        # if ("level" in ln) or ("buy_count" in ln) or ("price_sum" in ln):
        #     # numerator_list.append(str(i))
        #     numerator_dict[str(i)] = add_index #numerator_dict用来存储带有关键词的特征index，其值为新增特征的index
        #     add_index +=1
        #     ln += "_ratio"
        #     newfeature.append(ln)
    return (numerator_price,numerator_count,newfeature)

def fea_index(fea_all):
    fea_all_index = []
    for i in range(len(fea_all)):
        fea_all_index.append(valid_jsontxt(fea_all[i]) + "\t" + str(i + 1))
    return fea_all_index

def f(line,numerator_price,numerator_count):
    ss = line.strip().split('\001')
    features = ss[1].split()
    result = copy.copy(features)
    buy_price = float(features[0].split(":")[1])
    buy_count = float(features[1].split(":")[1])
    for ln in features:
        f_index = ln.split(":")[0]
        f_value = float(ln.split(":")[1])
        if numerator_price.has_key(f_index):
            new_index = numerator_price[f_index]
            new_value = str(round((f_value / buy_price),2))
            result.append(new_index + ":" + new_value)
        elif numerator_count.has_key(f_index):
            new_index = numerator_count[f_index]
            new_value = round((f_value / buy_count),2)
            result.append(valid_jsontxt(new_index) + ":" + valid_jsontxt(new_value))
    return valid_jsontxt(ss[0]) + "\001" + " ".join(result)


rdd = sc.textFile("/hive/warehouse/wlcredit.db/t_credit_feature_merge/ds=" + today + "_cms1234")
feature_raw = sc.textFile("/hive/warehouse/wlcredit.db/t_wrt_credit_all_features_name/ds=" + today + "_cms1234")\
    .map(lambda x:valid_jsontxt(x.split("\t")[0])).collect()
#统计原始特征数量
feature_raw_len = len(feature_raw)
feature_raw_len += 1
#提取原始特征中会用到的特征
numerator_price,numerator_count,newfeature = add_index(feature_raw,feature_raw_len)
fea_all_index = fea_index(newfeature)
sc.parallelize(fea_all_index).saveAsTextFile('/user/wrt/temp/add_new_feature_name')
rdd.map(lambda x:f(x,numerator_price,numerator_count)).saveAsTextFile('/user/wrt/temp/add_newfeature_inhive')

hiveContext.sql("load data inpath '/user/wrt/temp/add_new_feature_name' overwrite into table \
wlcredit.t_wrt_credit_all_features_name PARTITION (ds = '" + today + "_cms1234_anf')" )

hiveContext.sql("LOAD DATA INPATH '/user/wrt/temp/add_newfeature_inhive' OVERWRITE \
INTO TABLE wlcredit.t_credit_feature_merge PARTITION (ds = '" + today + "_cms1234_anf')" )

# cms代表cate_month_cross anf代表add_new_feature
# hfs -rmr /user/wrt/temp/add_new_feature_name && hfs -rmr /user/wrt/temp/add_newfeature_inhive
# spark-submit --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 add_new_feat.py 20170214_cms1234_anf








