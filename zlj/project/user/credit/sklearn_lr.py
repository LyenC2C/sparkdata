#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')



import numpy as np
import matplotlib.pyplot as plt
from sklearn import linear_model
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_curve, roc_auc_score
# gbdt=RandomForestClassifier()
# gbdt.predict_proba()

data=np.loadtxt(open(u'E:\\项目\\征信&金融\\模型\\train_data'),delimiter='\t')
# data=np.loadtxt(open('train_data'),delimiter='\t')
# logisReg = linear_model.LogisticRegression()


test=np.loadtxt(open(u'E:\\项目\\征信&金融\\模型\\test_data'),delimiter='\t')
Y = data[:, 0]
X=data[:,1:]

clf = GradientBoostingClassifier(n_estimators=100, learning_rate=1.0,  max_depth=2, random_state=0).\
    fit(X, Y)

# print clf.feature_importances_
#
test_y=test[:,0]
test_x=test[:,1:]

pred= clf.predict(test_x)
pred_p= clf.predict_proba(test_x)

for k,v in zip(test_y,pred):
    if k==1.0: print k,v


roc_test = roc_curve(test_y, pred_p[:,1])

# 作图

plt.plot(roc_test[0], roc_test[1])

plt.show()


# print sum([True for k,v in zip(test_y,pred) if k==v])
# x=test_x.reshape(-1,279)
# logisReg.fit(X,Y)
#
# pred=logisReg.predict(test_x)
# pred_p=logisReg.predict_proba(test_x)
# [ 1 for k,v in zip(test_y,pred) if k==v]
# #
#
# for j,k,v in zip (pred ,pred_p,test_y):
#     if v==1.0 :
#         print j,k[1],v
# 1109
# 1042