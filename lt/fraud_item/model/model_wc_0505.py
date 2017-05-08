#encoding:utf-8
__author__='lt'


#====================taobao item classification===========
from pyspark.mllib.feature import HashingTF,IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark import *
from pyspark.sql.functions import *

sc=SparkContext(appName="model_words")
hiveContext = HiveContext(sc)
hiveContext.sql('use wl_service')

#1)item keywords feature
data=hiveContext.sql('select * from wl_service.t_lt_train_items_v1')
keywords_dict = data.flatMap(lambda x:x.keywords.split('|')).distinct().zipWithIndex().collectAsMap()
num_words = len(keywords_dict)
def creat_vector(words,keywords_dict,num_words):
    x=[0]*num_words
    for w in words:
        if w in keywords_dict.keys():
            index = keywords_dict[w]
            x[index]=1
    return x

data_key = data.map(lambda x:(x.item_id,x.label,creat_vector(x.keywords.split('|'),keywords_dict,num_words)))

#2)item words tf
data = hiveContext.sql('select * from wl_service.t_lt_train_item_words_v2')
#words to vector
tf=HashingTF()
data_tf = data.map(lambda x:(x.item_id,x.label,tf.transform(x.words.split('_'))))


#3)item words tf_idf
features = data_tf.map(lambda x:x[-1])
idf=IDF().fit(features)
idf_FT = idf.transform(features)
#item_id,label,feature
data_tfidf = data_tf.map(lambda x:(x[0],x[1])).zip(idf_FT).map(lambda x:(x[0][0],x[0][1],x[1]))

#4)item info(price 和 favor特征需要标准化)
data_item=hiveContext.sql('select * from wl_service.t_lt_trian_item_info_v3')
data_item.na.fill(0)
data_item_ft=data_item.map(lambda x:(x.item_id,x.label,x[1:11]))


#5)shop info(price/favor/credit,特征需要标准化)
data_shop=hiveContext.sql('select * from wl_service.t_lt_trian_item_shop_info_v4')
data_shop.na.fill(0)
data_shop_ft = data_shop.map(lambda x:(x.item_id,x.label,x[1:23]))


def split_data(data,split_r=[0.8,0.2]):
    spam = data.filter(lambda x:x[1]==1) #51563
    normal = data.filter(lambda x: x[1] == 0)
    data_train=spam.randomSplit([0.8,0.2])[0].union(normal.randomSplit(split_r)[0])
    return data_train


#======================models===============================================
#Naive bayes
from pyspark.mllib.classification import NaiveBayesModel,NaiveBayes
def NB_train(data):
    data_train=split_data(data)
    # data_train,data_cv = data.randomSplit([0.8,0.2],0)
    key_FT = data_train.map(lambda x:LabeledPoint(x[1],x[-1]))
    training,test = key_FT.randomSplit([0.8,0.2],0)
    model_NB = NaiveBayes.train(training,0.1)
    predictionAndlabel = test.map(lambda x:(float(model_NB.predict(x.features)),x.label))
    accuracy=1.0*predictionAndlabel.filter(lambda (x,v):x==v).count()/test.count()
    print ("accuracy of model_NB:%f" % accuracy)
    return model_NB,accuracy


#Decision tree
from pysaprk.mllib.tree import DecisionTree
def DT_train(data):
    data_train = split_data(data)
    key_FT = data_train.map(lambda x:LabeledPoint(x[1],x[-1]))
    training,test = key_FT.randomSplit([0.8,0.2],0)
    model_DT = DecisionTree.trainClassifier(training,2,{})
    predictionAndlabel = test.map(lambda x:(float(model_DT.predict(x.features)),x.label))
    accuracy=1.0*predictionAndlabel.filter(lambda (x,v):x==v).count()/test.count()
    print ("accuracy of model_DT:%f" % accuracy)
    return model_DT,accuracy


from pyspark.mllib.classification import LogisticRegressionWithSGD
# numIterations = 10
def LR_train(data):
    data_train = split_data(data)
    key_FT = data_train.map(lambda x:LabeledPoint(x[1],x[-1]))
    training,test = key_FT.randomSplit([0.8,0.2],0)
    model_LR = LogisticRegressionWithSGD.train(training,10)
    predictionAndlabel = test.map(lambda x:(float(model_LR.predict(x.features)),x.label))
    accuracy=1.0*predictionAndlabel.filter(lambda (x,v):x==v).count()/test.count()
    print ("accuracy of model_LR:%f" % accuracy)
    return model_LR,accuracy

from pyspark.mllib.classification import SVMWithSGD
# numIterations = 10
def SVM_train(data):
    data_train = split_data(data)
    key_FT = data_train.map(lambda x: LabeledPoint(x[1], x[-1]))
    training, test = key_FT.randomSplit([0.8, 0.2], 0)
    model_SVM = SVMWithSGD.train(training, 10)
    predictionAndlabel = test.map(lambda x: (float(model_SVM.predict(x.features)), x.label))
    accuracy = 1.0 * predictionAndlabel.filter(lambda (x, v): x == v).count() / test.count()
    print ("accuracy of model_SVM:%f" % accuracy)
    return model_SVM, accuracy

from pyspark.mllib.tree import RandomForest
#trainClassifier(data, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy='auto', impurity='gini', maxDepth=4, maxBins=32, seed=None)
def RF_train(data):
    data_train = split_data(data)
    key_FT = data_train.map(lambda x: LabeledPoint(x[1], x[-1]))
    training, test = key_FT.randomSplit([0.8, 0.2], 0)
    model_RF = RandomForest.trainClassifier(training, 2, {}, 5, seed=42)
    predictionAndlabel = test.map(lambda x: (float(model_RF.predict(x.features)), x.label))
    accuracy = 1.0 * predictionAndlabel.filter(lambda (x, v): x == v).count() / test.count()
    print ("accuracy of model_RF:%f" % accuracy)
    return model_RF, accuracy


from pyspark.mllib.tree import GradientBoostedTrees
#trainClassifier(data, categoricalFeaturesInfo, loss='logLoss', numIterations=100, learningRate=0.1, maxDepth=3, maxBins=32)
def GBDT_train(data):
    data_train = split_data(data,[0.5,0.5])
    key_FT = data_train.map(lambda x: LabeledPoint(x[1], x[-1]))
    training, test = key_FT.randomSplit([0.8, 0.2], 0)
    model_GBDT = GradientBoostedTrees.trainClassifier(training, {})
    predictionAndlabel = test.map(lambda x: (float(model_GBDT.predict(x.features)), x.label))
    accuracy = 1.0 * predictionAndlabel.filter(lambda (x, v): x == v).count() / test.count()
    print ("accuracy of model_GBDT:%f" % accuracy)
    return model_GBDT, accuracy


def pre_all(data_all,model,filename):#item_id,label,feature
    all_FT = data_all.map(lambda x: (x[0], x[1], model.predict(x[-1])))
    return_data = all_FT.filter(lambda x: x[-1] != x[1])
    return_data.map(lambda x: "\001".join(["%s" % i for i in x])).saveAsTextFile("/user/lt/model_0508/%s"%(filename))
    return return_data


#trian using NB model
model_NB_keywords,accuracy_key =NB_train(data_key)
model_NB_tf,accuracy_tf=NB_train(data_tf)
model_NB_tfidf,accuracy_tfidf =NB_train(data_tfidf)
#predict
pre_all(data_key,model_NB_keywords,"model_NB_keywords")
pre_all(data_tf,model_NB_tf,"model_NB_tf")
pre_all(data_tfidf,model_NB_tfidf,"model_NB_tfidf")

#train GBDT
model_GBDT_item,accuracy_item =GBDT_train(data_item_ft)
model_GBDT_item_shop,accuracy_shop =GBDT_train(data_shop_ft)
pre_all(data_item_ft,model_GBDT_item,"model_GBDT_item")
pre_all(data_shop_ft,model_GBDT_item_shop,"model_GBDT_item_shop")


