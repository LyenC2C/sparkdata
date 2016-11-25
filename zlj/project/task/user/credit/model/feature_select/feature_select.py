#coding:utf-8
from sklearn.decomposition import PCA
from sklearn.utils import column_or_1d
from zlj.project.task.user.credit.model.test_1w.model_utils import data_abnormal

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
import sklearn
import collections as coll
from xgboost.sklearn import XGBClassifier
from sklearn.preprocessing import Imputer, StandardScaler

# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\test1w\\融360_v3back.csv')
file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_flow.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_2014.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_2015.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_std.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_std_avg.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\融360_v3back.csv')


mulddata= pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\multload.csv')
kv=coll.defaultdict(int)
for k,v in zip(mulddata['tel'],mulddata['hit']):
    kv[k]=v if kv[k]<v else kv[k]


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



import  math
for i  in ['total_price','avg_price','std_price']:
    file[i]=file[i].map(lambda x:math.log(x+0.1,2))

file['total_price_mean']=file['total_price']/(file['age']+1)
file['total_price_mean_month']=file['total_price']/(file['buy_month']+1)

print 'len(data):',len(file)

# data=      file[file['class'] != '2000_c' ['8000_c','data_2k']]
data=      file[file['class'] == '8000_c' ]
# data=      file
valid_data=file[file['class']=='2000_c']



print len(valid_data),len(data)

f=['cnt_ratio_50002766', 'price_ratio_50019095', 'cnt_ratio_50002768', 'price_ratio_50025110', 'cnt_ratio_40', 'cnt_ratio_124044001', 'cnt_ratio_50010404', 'cnt_ratio_50007218', 'cnt_ratio_50023878', 'cnt_ratio_50025705', 'car_flag', 'cnt_ratio_50010728', 'cnt_ratio_50012100', 'cnt_ratio_2813', 'price_ratio_50023722', 'price_ratio_120950002', 'cnt_ratio_50023575', 'cnt_ratio_50011397', 'house_flag', 'cnt_ratio_50026316', 'cnt_ratio_124242008', 'b_bc_type_num_ratio', 'price_ratio_124024001', 'brand_id_num', 'gender', 'cnt_ratio_50023804', 'cnt_ratio_50014927', 'cnt_ratio_1101', 'cnt_ratio_50011949', 'annoy_ratio', 'price_ratio_50014812', 'cnt_ratio_50006843', 'cnt_ratio_50006842', 'cnt_ratio_50012082', 'annoy_num', 'std_price_ratio', 'pet_flag', 'cnt_ratio_50010788', 'cnt_ratio_122650005', 'cnt_ratio_50024451', 'cnt_ratio_50020485', 'cnt_ratio_50013864', 'cnt_ratio_124050001', 'cnt_ratio_50026523', 'cnt_ratio_50008164', 'cnt_ratio_50008165', 'cnt_ratio_50025110', 'cnt_ratio_50025111', 'cnt_ratio_50012164', 'cnt_ratio_50022703', 'price_ratio_50018222', 'price_ratio_50025004', 'cnt_ratio_50026800', 'cnt_ratio_50050471', 'cnt_ratio_1801', 'child_flag', 'price_ratio_122950001', 'cnt_ratio_50016348', 'price_ratio_50023878', 'price_ratio_50023804', 'cnt_ratio_50008075', 'cnt_ratio_50018222', 'cnt_ratio_50020275', 'cnt_ratio_50019780', 'cnt_ratio_50014812', 'avg_price_ratio', 'cnt_ratio_50011972', 'cnt_ratio_124024001', 'cnt_ratio_50011699', 'cnt_ratio_50454031', 'cnt_ratio_122684003', 'cnt_ratio_122928002', 'cnt_ratio_50020808', 'price_ratio_40', 'cnt_ratio_50013886', 'price_ratio_50016891', 'cnt_ratio_50008141', 'cnt_ratio_50011665', 'cnt_ratio_50011740', 'cnt_ratio_50012029', 'price_ratio_124242008', 'cnt_ratio_50020579', 'cnt_ratio_50008090', 'price_ratio_50020611', 'price_ratio_50024612', 'cnt_ratio_50008907', 'b_bc_type_num', 'cnt_ratio_16', 'cnt_ratio_11', 'avg_cnt', 'cnt_ratio_50025004', 'cnt_ratio_50019095', 'cnt_ratio_50018004', 'price_ratio_50025705', 'b_bc_price_ratio', 'cnt_ratio_50004958', 'cnt_ratio_29', 'cnt_ratio_21', 'cnt_ratio_26', 'cnt_ratio_27', 'cat_id_num', 'cnt_ratio_122718004', 'cnt_ratio_50020857', 'price_ratio_25', 'cnt_ratio_50023282', 'local_buycount', 'cnt_ratio_25', 'cnt_ratio_50022517', 'root_cat_id_num', 'cnt_ratio_35', 'cnt_ratio_34', 'cnt_ratio_33', 'cnt_ratio_50016422', 'cnt_ratio_50018264', 'cnt_ratio_1512', 'cnt_ratio_121536007', 'price_ratio_50020808', 'cnt_ratio_122852001', 'cnt_ratio_50023904', 'cnt_ratio_122950001', 'cnt_ratio_50020332', 'cnt_ratio_50016891', 'cnt_ratio_124468001']
# 特征索引
index_data=data.iloc[:,3:]
# index_data=data.loc[:,f]
index_lable= data.loc[:,  ['label']]
print index_data.columns,len(index_data.columns)

test_featrue=valid_data.loc[:,index_data.columns]

print test_featrue.columns
print set(index_data.columns)-set(test_featrue.columns)

features=index_data.columns
'''
降维
'''
index_data=Imputer().fit_transform(index_data)
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

test_rflasso()


import os
os._exit(0)

# step=2     blag 0.747359870024
ls=[]
feature_kv=coll.defaultdict(int)
kflod=[]
# for step  in [6]:
