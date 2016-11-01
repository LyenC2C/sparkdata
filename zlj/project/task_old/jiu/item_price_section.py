__author__ = 'zlj'
# 类目价格区间

from pyspark.mllib.clustering import *
from pyspark.sql import *
from pyspark import SparkContext

import numpy as np

sc = SparkContext(appName="test")

sqlContext = SQLContext(sc)
path = "/user/zlj/data/iteminfo5"
df = sqlContext.read.json(path)

rdd = df.select('price')
rdd.filter(lambda x: x.price.isdigit()).map(lambda x: str(x.price)).saveAsTextFile("/user/zlj/data/temp/item_price")

pricerdd = sc.textFile("/user/zlj/data/temp/item_price")
# rdd.map(lambda  x:x.price).mean()
list = sc.textFile("/user/zlj/data/temp/item_price").filter(lambda x: '-' not in x).map(lambda x: float(x)).filter(
    lambda x: x > 20 and x < 4000).collect()

data = np.array(list).reshape(len(list), 1)

model = KMeans.train(
    sc.parallelize(data), 6, maxIterations=20, runs=30, initializationMode="random",
    seed=50, initializationSteps=5, epsilon=1e-4)

model.save(sc,'')
model.centers
model.predict()
# data = np.array([0.0,0.0, 1.0,1.0, 9.0,8.0, 8.0,9.0]).reshape(4, 2)


# [array([ 447.65030601]), array([ 1198.97089491]), array([ 3136.93138045]), array([ 110.58036037]), array([ 7301.34688859])]

# lambda x:x>20 and x<2000
# [array([ 75.8169773]), array([ 461.30277905]), array([ 1518.02082377]), array([ 832.59729243]), array([ 227.38496293])]
# model.centers
# model.clusterCenters
# model.k

# 作图
rdd.map(lambda x: str(x.price)).filter(lambda x: '-' not in x).map(lambda x: float(x)).histogram(
    [i * 100 for i in xrange(100)])
# ([0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000, 2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900, 3000, 3100, 3200, 3300, 3400, 3500, 3600, 3700, 3800, 3900, 4000, 4100, 4200, 4300, 4400, 4500, 4600, 4700, 4800, 4900, 5000, 5100, 5200, 5300, 5400, 5500, 5600, 5700, 5800, 5900, 6000, 6100, 6200, 6300, 6400, 6500, 6600, 6700, 6800, 6900, 7000, 7100, 7200, 7300, 7400, 7500, 7600, 7700, 7800, 7900, 8000, 8100, 8200, 8300, 8400, 8500, 8600, 8700, 8800, 8900, 9000, 9100, 9200, 9300, 9400, 9500, 9600, 9700, 9800, 9900],
# [93106, 47890, 22727, 14845, 7601, 6349, 4752, 2766, 2612, 2167, 1064, 1012, 1034, 739, 332, 587, 706, 322, 438, 535, 132, 153, 234, 218, 95, 167, 164, 78, 160, 235, 62, 48, 71, 58, 30, 118, 101, 30, 133, 141, 42, 35, 33, 28, 17, 37, 34, 22, 73, 49, 25, 14, 11, 23, 14, 12, 22, 6, 59, 68, 8, 5, 6, 7, 7, 28, 31, 6, 48, 21, 4, 3, 11, 4, 2, 8, 11, 5, 23, 23, 17, 2, 3, 7, 2, 8, 7, 1, 58, 33, 11, 5, 4, 1, 2, 9, 4, 3, 21])

# [array([ 475.96532742]), array([ 3085.02551724]), array([ 75.89932408]), array([ 881.4674328]), array([ 1666.51726623]), array([ 229.92416632])]
user_price_avg = sqlContext.read.json('/user/zlj/data/user_wt_his/user_price_avg/')


def change_weight(v):
    x = -1
    if v == 0:
        x = 3
    elif v == 1:
        x = 6
    elif v == 2:
        x = 1
    elif v == 3:
        x = 4
    elif v == 4:
        x = 5
    elif v == 5:
        x = 2
    return x
dic={0:3,
     1:6,
     2:1,
     3:4,
     4:5,
     5:2}

upavg = user_price_avg.map(lambda x: (x.uid, x.price_avg, dic[model.predict([x.price_avg])]))
upavg.map(lambda x: " ".join([str(i) for i in x])).saveAsTextFile('/user/zlj/data/user_price_section')