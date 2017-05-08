"""
 model train using taobao item features
 author:lt
 created on:20170418
 pyspark --driver-memory 6g --executor-memory 15g --executor-cores 5 --num-executors 15
 spark2-submit --num-executors 15 ./classify_model.py
"""


from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF,IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayesModel,NaiveBayes
from pyspark.mllib.evaluation import BinaryClassificationMetrics
import pyspark.sql.functions as F
from pyspark import SQLContext


1.#rdd format
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext

sc=SparkContext(appName="modelTrain")
data = sc.textFile("/hive/warehouse/wl_service.db/t_lt_base_sp_item_fraud_train/d340*")
data_item = data.map(lambda x:x.split("\001"))

spam = data_item.filter(lambda x:x[-1]=='1')
normal = data_item.filter(lambda x:x[-1]=='0')

spam_cv = sc.parallelize(spam.takeSample(False,10000,1))
normal_cv= sc.parallelize( normal.takeSample(False,100000,1))

spam_feature = spam.map(lambda x:LabeledPoint(x[-1],[x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[9],x[10]]))
normal_feature = normal.map(lambda x:LabeledPoint(x[-1],[x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[9],x[10]]))
sample_train = spam_feature.union(normal_feature)

training,test = sample_train.randomSplit([0.8,0.2],seed=0)

#LR model
from pyspark.mllib.classification import LogisticRegressionWithSGD,LogisticRegressionWithLBFGS
modelLR = LogisticRegressionWithSGD.train(training, 10)
predictionAndlabel =test.map(lambda p: (float(model.predict(p.features)),p.label))
accuracy_lr = 1.0*predictionAndlabel.filter(lambda (x,v): x==v).count()/test.count()


2.#dataframe fromat
from pyspark.mllib.regression import LabeledPoint
from pyspark import SQLContext

# 1)  numerical feature
data = sqlContext.sql("select item_id,fraud_score_1,fraud_score_11,fraud_score_2,fraud_score_22,fraud_score_3,fraud_score_4,price,favor,label from wl_service.t_lt_base_sp_item_fraud_train")
data.na.fill(0)

#split train and cv sample
data_train,data_cv = data.randomSplit([0.8,0.2],0)

#spam=data_train.filter(data_train.label==1)

data_feature = data_train.map(lambda x:LabeledPoint(x.label,x[1:-1]))
train,test = data_feature.randomSplit([0.8,0.2],seed=0)

#LR model
from pyspark.mllib.classification import LogisticRegressionWithSGD,LogisticRegressionWithLBFGS
model_LR = LogisticRegressionWithSGD.train(train,6)
predictLabel = test.map(lambda p:(float(model_LR.predict(p.features)),p.label))
accuracy =1.0*predictLabel.filter(lambda (x,v):x==v).count()/test.count()

#cv
cv_result = data_cv.map(lambda x:(x.item_id,model_LR.predict(x[1:-1]),x.label))
result = cv_result.filter(lambda x:x[1]!=x[-1])
out_data = result.map(lambda x:str(x[0])+'\001'+str(x[1])+'\001'+str(x[2]))
out_data.saveAsTextFile('/user/lt/lr_pre')

sqlContext.registerDataFrameAsTable(out_data, "table1")
table = sqlContext.table("table1")


# 2) all feature
df= sqlContext.sql("select * from wl_service.t_lt_base_sp_item_fraud_train")
df_keywords = df.select("item_id","keywords")
df_bc_type = df.select("item_id","bc_type")
df_starts = df.select("item_id","starts")

#words to feature
keywords_rdd = df_keywords.rdd
tf = HashingTF()
keywords_rdd_1 = keywords_rdd.map(lambda x:tf.transform(x[1].split("|")))
words = df.select("keywords").distinct().rdd.flatMap(lambda x:x.split("|")).collect()
words_index = tf.transform(words)

#dummy
rc_names = df.select("rc_name").distinct().rdd.flatMap(lambda x:x).collect()
rc_exp = [F.when(F.col("rc_name")== name,1).otherwise(0).alias("rc_name"+name) for name in rc_names]
df = df.select("item_id","rc_name",*rc_exp)
df.show()


