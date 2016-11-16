#coding:utf-8
from sklearn.utils import column_or_1d

__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import xgboost as xgb
from xgboost.sklearn import XGBClassifier
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





file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\融360_v3back.csv')

'''
data clean
'''
# print file.columns[3:]
for i in file.columns[3:]:
    file[i]=file[i].map(lambda x:data_abnormal(x))

import  math
for i  in ['total_price','avg_price','std_price']:
    file[i]=file[i].map(lambda x:math.log(x+0.1,2))
print 'len(data):',len(file)

data=      file[file['class']=='8000_c']
# data=      file
valid_data=file[file['class']=='2000_c']



# 特征索引
index_data=data.iloc[:,3:]
features=data.columns[3:]
index_lable= data.loc[:,  ['label']]
print index_data.columns,len(index_data.columns)

test_featrue=valid_data.loc[:,index_data.columns]


print test_featrue.columns
print set(index_data.columns)-set(test_featrue.columns)


# 特征交叉
cross_flag=0
index_data=feature_cross(index_data) if cross_flag==1 else index_data

print index_data.columns
eta=0.05
max_depth=4
min_child_weight=1
subsample=1.0
colsample_bytree=0.8
alpha=1
llambda=200
gamma=0.2
round=200

def xgb_origin(train_X,train_Y):
    params = {'booster':'gblinear','nthread':4,
    "objective": "binary:logistic",  "eta": eta,"max_depth": max_depth,
                 "min_child_weight": min_child_weight, "silent": 0, "subsample": subsample,
                               "colsample_bytree":colsample_bytree,  "seed": 1,"alpha":alpha,"lambda":llambda}
    xgb.train(params,xgb.DMatrix(train_X,label=train_Y) ,num_boost_round=round).predict(xgb.DMatrix(test_X))

def xgb_sk(train_X,train_Y):
    xgbsk=XGBClassifier(max_depth=max_depth, learning_rate=eta,
                 n_estimators=round, silent=True,
                 objective="binary:logistic",
                 nthread=-4, gamma=0, min_child_weight=1,
                 max_delta_step=0, subsample=subsample, colsample_bytree=colsample_bytree, colsample_bylevel=1,
                 reg_alpha=alpha, reg_lambda=llambda, scale_pos_weight=1,seed=0)
    xgb_model=xgbsk.fit(train_X,train_Y,eval_metric=['logloss','auc'])
    return xgb_model

# 特征分析
def feature_anay(features,feature_importances_):
    pair=zip(features,feature_importances_)
    data=sorted(pair,key=lambda t:t[-1],reverse=True)
    return data[:20]

for step  in xrange(10):
    print '---------------------',step

    # print list(index_data.columns)
    train_X,test_X,train_Y,test_Y=train_test_split(index_data,index_lable ,  test_size=0.3 , random_state=step)
    print len(train_X),len(test_X)
    print sum(train_Y['label']),sum(test_Y['label'])

    # print xgb.train(params,xgb.DMatrix(train_X,label=train_Y) ,num_boost_round=round).predict(xgb.DMatrix(test_X))
    # xgb_pred=xgb.train(params,xgb.DMatrix(train_X,label=train_Y) ,num_boost_round=200).predict(xgb.DMatrix(test_X))
    model=xgb_sk(train_X,train_Y)
    xgb_pred=model.predict_proba(test_X)[:,1]
    print feature_anay(index_data.columns,model.feature_importances_)
    xgb_rs=model_rs_dataframe(test_Y,xgb_pred)
    xgb_auc=metrics.roc_auc_score(test_Y['label'], xgb_rs['rs'])
    xgb_ks=np.array([ i for  i in ks_calc(xgb_rs)['ks']]).max()
    print  'xgb', xgb_auc,xgb_ks


