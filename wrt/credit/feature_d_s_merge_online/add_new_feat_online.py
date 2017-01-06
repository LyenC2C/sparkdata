#coding:utf-8
__author__ = 'wrt'
#此脚本目的主要是5000个特征基础上加入一些占比特征
import sys
import rapidjson as json
import copy
from pyspark import SparkContext
from pyspark.sql import *

today = sys.argv[1]

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

def need_index(feature5k):
    numerator_price = {}
    numerator_count = {}
    newfeature = copy.copy(feature5k)
    # add_list.append("1") #monthall_buy_count
    # add_list.append("2") #monthall_price_sum
    add_index = 5000
    for i in range(3,len(feature5k)):
        ln = valid_jsontxt(feature5k[i])
        if ("sum_price_level" in ln) or ("price_sum" in ln):
            numerator_price[str(i)] = str(add_index)
            newfeature.append(ln)
            add_index += 1
        elif ("count_level" in ln) or ("buy_count" in ln):
            numerator_count[str(i)] = str(add_index)
            newfeature.append(ln)
            add_index += 1
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
        fea_all_index.append(valid_jsontxt(fea_all[i]) + "\t" + str(i))
    return fea_all_index

def f(line,numerator_price,numerator_count):
    ss = line.strip().split('\001')
    features = ss[1].split()
    result = copy.copy(features)
    buy_count = features[1]
    buy_price = features[2]
    for ln in features:
        f_index = ln.split(":")[0]
        f_value = ln.split(":")[1]
        if numerator_price.has_key(f_index):
            new_index = numerator_price[f_index]
            new_value = str(round((f_value / buy_price),2))
            result.append(new_index + ":" + new_value)
        elif numerator_count.has_key(f_index):
            new_index = numerator_count[f_index]
            new_value = round((f_value / buy_count),2)
            result.append(new_index + ":" + new_value)
    return ss[0] + "\001" + " ".join(result)


rdd = sc.textFile("/hive/warehouse/wlcredit.db/t_credit_feature_merge/ds=20170105_online")
feature5k = sc.textFile("/user/wrt/feature_5k").collect()
#提取5k个特征中会用到的特征
numerator_price,numerator_count,newfeature = add_index(feature5k)
fea_all_index = fea_index(newfeature)
sc.parallelize(fea_all_index).saveAsTextFile('/user/wrt/temp/add_new_feature_name')
rdd.map(lambda x:f(x)).saveAsTextFile('/user/wrt/temp/add_newfeature_inhive')

# spark-submit --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120







