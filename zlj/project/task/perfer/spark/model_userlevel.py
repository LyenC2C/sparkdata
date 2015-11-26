__author__ = 'zlj'

from pyspark.sql import *
from pyspark.mllib.clustering import *
from pyspark import SparkContext

from numpy import array

sc=SparkContext(appName="test")
sqlContext = SQLContext(sc)




sqlContext.read.json('/user/hadoop/qq/info/qqinfo.total')




data=sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_ec_perfer_priceavg/',90)\
    .map(lambda x :[int(float(i)) for index ,i in enumerate(x.split('\001')) if index>0])
data.cache()
# data.filter(lambda  x: x[2]<10000).histogram(1000)


# path='/hive/warehouse/wlbase_dev.db/t_zlj_ec_perfer_priceavg/'

#���˾���10000 ���ϵģ��д�����


s1=data.filter(lambda x:x[2]<10000).map(lambda x:array(x))
model = KMeans.train(
    s1, 5, maxIterations=20, runs=30, initializationMode="random",
    seed=50, initializationSteps=5, epsilon=1e-4)

# model.centers


level_map={0:4,1:3,2:2,3:5,4:1}

def fun(line,model):
    a=[]
    values=line.split('\001')
    level=model.predict(array([int(float(i)) for i in values[1:]]))
    a.extend(values)
    a.append(str(level_map[level]))
    return '\001'.join(a)
min=sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_ec_perfer_priceavg/',90).map(lambda x :fun(x,model))\
    .saveAsTextFile('/user/zlj/data/perfer/user_consumeLevel')



# model.predict(array([8,5000,500]))
# model.predict(array([8,5000,500]))
# [array([ 4530.60270876]), array([ 1873.43699654]), array([ 704.2066297]), array([ 200.46748559]), array([ 41.90180564])]

# [array([   10.37059271,  7038.45867955,  2204.59502341]), 4
# array([    6.02294698,  2524.62728243,  1025.96938381]), 3
# array([   5.06144003,  714.9711768 ,  255.84548934]), 2
# array([    42.1044747 ,  22376.08621699,   2638.86989428]),5
# array([  2.06277612,  99.15790257,  55.6349806 ])]  1