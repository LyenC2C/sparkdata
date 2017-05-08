#encoding:utf-8

from pyspark.mllib.feature import HashingTF,IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayesModel,NaiveBayes
from pyspark import *

sc=SparkContext(appName="model_words")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

sql_item='''
select * from wl_service.t_lt_train_item_words_v2
'''
data = hiveContext.sql(sql_item)

#words to vector
tf=HashingTF()
data_tf = data.map(lambda x:(x[0],x[1],x[2],tf.transform(x[-1].split('_'))))

#tfidf
idf=IDF().fit(data_tf.map(lambda x:x[-1]))
data_tfidf =data_tf.map(lambda x:(x[0],x[1],x[2],idf.transform(x[-1].split('_'))))

#split train and cv
data_train,data_cv = data_tf.randomSplit([0.8,0.2],0)

#model NB trian
train_FT = data_train.map(lambda x:LabeledPoint(x[1],x[-1]))
training,test = train_FT.randomSplit([0.8,0.2],0)
model_NB = NaiveBayes.train(training,0.1)

#model DT trian


#test
predictionAndlabel = test.map(lambda p: (float(model_NB.predict(p.features)), p.label))
accuracy = 1.0 * predictionAndlabel.filter(lambda (x, v): x == v).count() / test.count()


#cv
cv_FT = data_cv.map(lambda x:(x[0],x[1],x[2],model_NB.predict(x[-1])))
return_data = cv_FT.filter(lambda x:x[-1]!=x[-2])





