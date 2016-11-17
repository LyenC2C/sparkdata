#coding:utf-8
from sklearn.utils import column_or_1d

__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')



import numpy as np
import matplotlib.pyplot as plt
from sklearn import linear_model, preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from   sklearn import  metrics
from sklearn.metrics import roc_curve, roc_auc_score
from sklearn.cross_validation import train_test_split
import pandas as pd
from pandas import Series, DataFrame
import warnings
warnings.filterwarnings("ignore")
from model_utils import *
import sklearn
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\test1w\\融360_v3back.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat.csv')
file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_std.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\融360_v3back.csv')

'''
data clean
'''
for i in file.columns[3:]:
    file[i]=file[i].map(lambda x:data_abnormal(x))
    # if 'cnt_ratio' in i or 'price_ratio' in i :
    #     col=file[i]
    #     min, max=col.min(),math.log(col.max()+1,2)
    #     file[i]=(col - min) / (min - max)
import  math
for i  in ['total_price','avg_price','std_price']:
    file[i]=file[i].map(lambda x:math.log(x+0.1,2))


import  math


file['total_price_mean']=file['total_price']/(file['age']+1)
file['total_price_mean_month']=file['total_price']/(file['buy_month']+1)

print 'len(data):',len(file)

# data=      file[file['class'] != '2000_c' ['8000_c','data_2k']]
data=      file[file['class'] != '2000_c' ]
# data=      file
valid_data=file[file['class']=='2000_c']



# 特征索引
index_data=data.iloc[:,3:]
index_lable= data.loc[:,  ['label']]
print index_data.columns,len(index_data.columns)

test_featrue=valid_data.loc[:,index_data.columns]


print test_featrue.columns
print set(index_data.columns)-set(test_featrue.columns)


# 特征交叉
# index_data=feature_cross(index_data)
# test_featrue=feature_cross(test_featrue)

# 划分训练测试集
from sklearn.preprocessing import Imputer, StandardScaler
features=index_data.columns

# step=2     blag 0.747359870024
ls=[]
for step  in xrange(10):
    print '---------------------',step
    train_X,test_X,train_Y,test_Y=train_test_split(index_data,index_lable ,  test_size=0.25 , random_state=step)
    from imblearn.combine import SMOTEENN,SMOTETomek
    from imblearn.over_sampling import *
    # sm = SMOTE()
    sm = SMOTEENN()
    # sm = SMOTETomek()
    # sm = ADASYN()

    print len(train_X),len(test_X)
    print sum(train_Y['label']),sum(test_Y['label'])
    train_X = Imputer().fit_transform(train_X)
    test_X = Imputer().fit_transform(test_X)
    # train_X, train_Y = sm.fit_sample(train_X, train_Y)

    # logisReg = linear_model.LogisticRegression(penalty='l2',solver='liblinear' ,n_jobs=4
    #                                            ,
    #                                            class_weight='balanced' ,max_iter=100, tol=0.0001
    #                                            )
    logisReg = linear_model.LogisticRegression(penalty='l2',
                                               solver='liblinear' ,n_jobs=4,
                                               # class_weight={1:0.8, 0:0.2}
                                               class_weight='balanced'
                                               )

    # from sklearn.feature_selection import SelectFromModel
    # df=SelectFromModel(logisReg,threshold=0.5).fit(train_X,train_Y)
    # mask=df._get_support_mask()
    # print mask

    # print [v  for k,v in zip(mask,features) if k==True]

    weight=np.array([i*5 if i==1 else 1 for i in list(train_Y)])
    lr =logisReg.fit(train_X,train_Y)
    print len(lr.coef_[0])

    f_value= np.sum(np.abs(lr.coef_), axis=0)
    # print f_value
    print feature_anay(features,f_value,20)
    # print sklearn.feature_selection()._get_feature_importances(lr)
    lr_pred_p= lr.predict_proba(test_X)
    lr_rs=model_rs_dataframe(test_Y,lr_pred_p[:,1])
    lr_auc=metrics.roc_auc_score(test_Y, lr_rs['rs'])
    lr_ks=np.array([ i for  i in ks_calc(lr_rs)['ks']]).max()
    print 'lr'   ,lr_auc ,lr_ks
    ls.append([lr_auc, lr_ks])
df= pd.DataFrame(ls)
print df.mean(axis=0)


