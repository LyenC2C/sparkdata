#coding:utf-8
from sklearn.decomposition import PCA
from sklearn.utils import column_or_1d


__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')
import os



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
from  pandas_utils import *
import sklearn
import collections as coll
from xgboost.sklearn import XGBClassifier
from sklearn.preprocessing import Imputer, StandardScaler

# file = pd.read_csv(u'E:\\项目\\1-征信&金融\\模型\\test1w\\融360_v3back.csv')
file = pd.read_csv(u'E:\\项目\\1-征信&金融\\模型\\rong360\\fix\\record_label_v_cat.csv')
# file = pd.read_csv(u'E:\\项目\\1-征信&金融\\模型\\rong360\\fix\\record_label_v_cat_flow.csv')
# file = pd.read_csv(u'E:\\项目\\1-征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_2014.csv')
# file = pd.read_csv(u'E:\\项目\\1-征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_2015.csv')
# file = pd.read_csv(u'E:\\项目\\1-征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_std.csv')


# file['mult_load']= [kv[i] if kv[i]>0 else 0 for i in file['tel']  ]

'''
data clean
'''
for i in file.columns[3:]:
    file[i]=file[i].map(lambda x:data_abnormal(x))
    if 'cnt_ratio' in i or 'price_ratio' in i :
        col=file[i]
        min, max=col.min(),math.log(col.max()+1,2)
        file[i]=(col - min) / (min - max)

import  math

for i  in ['total_price','avg_price','std_price']:
    file[i]=file[i].map(lambda x:math.log(x+0.1,2))

file['total_price_mean']=file['total_price']/(file['age']+1)
file['total_price_mean_month']=file['total_price']/(file['buy_month']+1)

print 'len(data):',len(file)


# 清理特征占比小于0.03 的特征
t=(file<=0).sum(axis=0)/len(file)
drop_f_list=filter_feature(t,0.96)
print 'drop f ',drop_f_list
file.drop(drop_f_list,axis=1,inplace=True)

print file.columns

rank_size=10
for  col in file.columns[5:]:
    v=file[col].map(lambda x:data_abnormal(x))
    size=int(len(file)/10)
    file['rn_'+col]=v.rank(method='max')/int(len(file)/rank_size)
    file['rn_'+col].astype(int)

# rank 10等分 计数特征
for i in xrange(rank_size):
    j=i+1
    file['n_'+str(j)]=(file==j).sum(axis=1)


# data=      file[file['class'] != '2000_c' ['8000_c','data_2k']]
data=      file[file['class'] == '8000_c' ]



# data=      file
valid=file[file['class']=='2000_c']

# print valid.iloc[:3].columns
valid_data=valid.iloc[:,3:]
#

# 特征索引
index_data=data.iloc[:,3:]

index_lable= data.loc[:,  ['label']]
print index_data.columns,len(index_data.columns)

features=index_data.columns
'''
降维
'''
index_data=Imputer().fit_transform(index_data)
valid_data=Imputer().fit_transform(valid_data)



print index_data.shape
print valid_data.shape
# pca=PCA(50)
# # pca=IncrementalPCA(200)
# pca.fit(index_data)
# index_data=pca.transform(index_data)

# 特征交叉
# index_data=feature_cross(index_data)
# test_featrue=feature_cross(test_featrue)

# 划分训练测试集


def test_rflasso():
    train_X,test_X,train_Y,test_Y=train_test_split(index_data,index_lable ,  test_size=0.25, random_state=1)
    from sklearn.linear_model import LogisticRegression
    from sklearn.feature_selection import SelectFromModel
    from sklearn.svm import SVC
    from sklearn.cross_validation import StratifiedKFold
    from sklearn.linear_model import RandomizedLogisticRegression
    randomized_logistic = RandomizedLogisticRegression(C=0.1,n_jobs=2)
    randomized_logistic.fit(train_X,train_Y)
    XX = randomized_logistic.transform(train_X)
    print XX.shape

# test_rflasso()
#
#
# import os
# os._exit(0)

# step=2     blag 0.747359870024
ls=[]
feature_kv=coll.defaultdict(int)
kflod=[]
# for step  in [6]:
result_preb=pd.DataFrame({'tel':valid['tel']})
text_preb  =pd.DataFrame()
for step  in  xrange(10):
    print '---------------------',step
    train_X,test_X,train_Y,test_Y=train_test_split(index_data,index_lable ,  test_size=0.25, random_state=step)
    from imblearn.combine import SMOTEENN,SMOTETomek
    if  'label' not in text_preb.columns: text_preb['label']=test_Y['label']
    from imblearn.over_sampling import *
    sm = SMOTE()
    # sm = SMOTEENN()
    # sm = SMOTETomek()
    # sm = ADASYN()

    print len(train_X),len(test_X) ,len(train_Y),len(test_Y),len(train_X)+len(test_X)
    print sum(train_Y['label']),sum(test_Y['label']),sum(train_Y['label'])+sum(test_Y['label'])
    logisReg = linear_model.LogisticRegression(penalty='l1',
                                               C=1,
                                               solver='liblinear' ,n_jobs=6,
                                               # class_weight={1:0.9, 0:0.1}
                                               class_weight='balanced'
                                               )

    weight=np.array([i*5 if i==1 else 1 for i in list(train_Y)])
    lr =logisReg.fit(train_X,train_Y)
    print len(lr.coef_[0])

    f_value= np.sum(np.abs(lr.coef_), axis=0)
    # print f_value
    top_features=feature_anay(features,f_value,50)

    for k,v  in top_features:
        feature_kv[k]=feature_kv[k]+v
    # print top_features
    # print sklearn.feature_selection()._get_feature_importances(lr)
    lr_pred_p= lr.predict_proba(test_X)
    print type(lr_pred_p[:,1])
    kflod.append(lr_pred_p[:,1])

    text_preb[str(step)]=lr.predict_proba(test_X)[:,1]
    result_preb[str(step)]=lr.predict_proba(valid_data)[:,1]

    lr_rs=model_rs_dataframe(test_Y ,lr_pred_p[:,1])
    lr_rs.to_csv(u'E:\\项目\\1-征信&金融\\模型\\rong360\\fix\\step9_ks.csv')
    lr_auc=metrics.roc_auc_score(test_Y , lr_rs['rs'])
    lr_ks=np.array([ i for  i in ks_calc(lr_rs)['ks']]).max()
    print 'lr'   ,lr_auc ,lr_ks
    ls.append([lr_auc, lr_ks])


text_preb['final']=text_preb.iloc[:,1:].mean(axis=1)
text_preb.to_csv(u'E:\\项目\\1-征信&金融\\模型\\rong360\\fix\\text_preb.csv')

result_preb['final']=result_preb.iloc[:,1:].mean(axis=1)
result_preb.to_csv(u'E:\\项目\\1-征信&金融\\模型\\rong360\\fix\\result_preb.csv')


lr_rs=model_rs_dataframe(text_preb ,text_preb['final'])
lr_auc=metrics.roc_auc_score(text_preb.loc[:,'label'], lr_rs['rs'])
lr_ks=np.array([ i for  i in ks_calc(lr_rs)['ks']]).max()
print  'final', lr_auc,lr_ks

df= pd.DataFrame(ls)
print df.mean(axis=0)

#lr.fit(X,y)
# grid_search_params = {'penalty':['l1','l2'],
#                       'C':[1,2,3,4]}
# import scipy.stats as st
# random_search_params = {'penalty':['l1','l2'],
#                       'C':st.randint(1,4)}
# #fit the classifier
# from sklearn.grid_search import GridSearchCV, RandomizedSearchCV
# gs = GridSearchCV(lr, grid_search_params)
# gs.fit(X, y)
# rs = RandomizedSearchCV(lr, random_search_params)
# rs.fit(X, y)
# gs.grid_scores_
# rs.grid_scores_
# max(gs.grid_scores_,key=lambda x : x[1])
# max(rs.grid_scores_,key=lambda x : x[1])