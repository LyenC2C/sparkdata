"""
 Naive Bayes mdoel for fraud title
 author:lt
 created on:20170406
 pyspark2 --driver-memory 6g --executor-memory 15g --executor-cores 5 --num-executors 15
 spark2-submit --num-executors 15 ./classify_model.py
"""

from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayesModel, NaiveBayes
from pyspark.mllib.evaluation import BinaryClassificationMetrics

sc = SparkContext(appName="NaiveBayesModel")


def Naive_bayes(data):
    print('model training')
    # split train and test
    training, test = data.randomSplit([0.8, 0.2], seed=0)
    modelNB = NaiveBayes.train(training, 1.0)
    predictionAndlabel = test.map(lambda p: (float(modelNB.predict(p.features)), p.label))
    accuracy = 1.0 * predictionAndlabel.filter(lambda (x, v): x == v).count() / test.count()
    print ("accuracy:%f" % accuracy)
    metrics = BinaryClassificationMetrics(predictionAndlabel)
    # Area under precision-recall curve
    print("Area under PR = %s" % metrics.areaUnderPR)
    # Area under ROC curve
    print("Area under ROC = %s" % metrics.areaUnderROC)
    # Save and load model
    modelNB.save(sc, "/user/lt/model/myNaiveBayesModel")
    print('save model ok!')
    # sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
    return modelNB


def predict_sample(samples, model):
    print("predict samples begain!")
    prelabel = samples.map(lambda p: (model.predict(p.features), int(p.label)))
    preSpam = prelabel.filter(lambda x: x[1] == 1)
    preSpamStr = preSpam.map(lambda (x, y): str(x) + '\001' + str(y))
    preSpamStr.saveAsTextFile('/user/lt/PreSpam')
    print("save predict results ok!")


if __name__ == '__main__':
    # path = '/user/lt/BigramAll/*'
    path = '/user/lt/Hanlp_fenci/*'
    datas = sc.textFile(path).map(lambda x: x.split('\t'))  # lable \t itemid \t words
    spam = datas.filter(lambda x: x[0] == '1')
    normal = datas.filter(lambda x: x[0] == '0')

    # feature vector
    tf = HashingTF()
    spamFeatrue = spam.map(lambda x: LabeledPoint(1, tf.transform(x[-1].split(" "))))
    # normal_train = normal.sample(withReplacement=False,fraction=0.005)
    normalFeatrue = normal.map(lambda x: LabeledPoint(0, tf.transform(x[-1].split(" "))))

    data = spamFeatrue.union(normalFeatrue)
    model_NB = Naive_bayes(data)

    # normal_test = normal.subtract(normal_train)
    pre_sample = normal.map(lambda x: LabeledPoint(x[1], tf.transform(x[-1].split(' '))))
    predict_sample(pre_sample, model_NB)


