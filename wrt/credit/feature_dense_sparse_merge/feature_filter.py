#coding:utf-8
__author__ = 'wrt'
from pyspark.mllib.stat import MultivariateStatisticalSummary
from pyspark.mllib.stat import Statistics
from pyspark.mllib.util import MLUtils
from pyspark.ml.feature import VectorSlicer
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import Vector
from pyspark import SparkContext
from pyspark.sql import *
import numpy
import math
import sys

today = sys.argv[1]
sc = SparkContext(appName="feature_filter")

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(x):
    ss = x.strip().split()
    result = []
    result.append(ss[0].strip().split('.')[0])
    for ln in ss[1:]:
        if float(ln.split(':')[1]) == 0.0: continue
        result.append(ln)
    return " ".join(result)

rdd = sc.textFile("/hive/warehouse/wlcredit.db/t_credit_feature_merge/ds=" + today + "_cms1234_anf")
rdd1 = rdd.map(lambda x:x.split("\001")[0] + " " + x.split("\001")[1])
rdd1.saveAsTextFile("/user/wrt/credit/allexample.libsvm")

data_svm_sql = sqlContext.read.format("libsvm").load("/user/wrt/credit/allexample.libsvm")
data_svm = data_svm_sql.map(lambda row:LabeledPoint(int(row.label),row.features))
features = data_svm.map(lambda x: x.features)
stat = Statistics.colStats(features)
coverage = (stat.numNonzeros()/stat.count()).tolist()
std = numpy.sqrt(stat.variance()).tolist()
features_nums = data_svm.map(lambda x: x.features.size).take(1)[0]
features_arr = range(0, features_nums)	
re = zip(zip(coverage,std),features_arr)
filteredIndexes = map(lambda m: m[1],filter(lambda a:a[0][0] >=0.005,re))
slicer = VectorSlicer(inputCol="features", outputCol="featuresFiltered", indices=filteredIndexes)
output_df = slicer.transform(data_svm_sql)
data_svm_filtered = output_df.select("label","featuresFiltered")
data_svm_labelpoint = data_svm_filtered.map(lambda row:LabeledPoint(int(row.label),row.featuresFiltered))
MLUtils.saveAsLibSVMFile(data_svm_labelpoint,"/user/wrt/credit/allexample_filter.libsvm")
rdd_r = sc.textFile("/user/wrt/credit/allexample_filter.libsvm")\
    .map(lambda x :x.split()[0].split('.')[0] + ' ' + ' '.join(x.split()[1:]))
rdd_r.saveAsTextFile("/user/wrt/credit/allexample_filter_telindex_features")
feature_raw = sc.textFile("/hive/warehouse/wlcredit.db/t_wrt_credit_all_features_name/ds=" + today + "_cms1234_anf")\
    .map(lambda x:valid_jsontxt(x.split("\t")[0])).collect()
fea_all_index = []
j = 1
for i in filteredIndexes:
    fea_all_index.append(feature_raw[i] + "\t" + str(j))
    j += 1
sc.parallelize(fea_all_index).saveAsTextFile('/user/wrt/temp/filter_feature_name')


hiveContext.sql("load data inpath '/user/wrt/temp/filter_feature_name' overwrite into table \
wlcredit.t_wrt_credit_all_features_name PARTITION (ds = '" + today + "_cms1234_anf_filter')")
hiveContext.sql("LOAD DATA INPATH '/user/wrt/credit/allexample_filter_telindex_features' OVERWRITE \
INTO TABLE wlcredit.t_credit_feature_merge PARTITION (ds = '" + today + "_cms1234_anf_filter')")