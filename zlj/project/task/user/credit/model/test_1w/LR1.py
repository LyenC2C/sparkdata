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
import collections as coll

# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\test1w\\融360_v3back.csv')
file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\chi_merge.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_flow.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_2014.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_2015.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_std.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k_std_avg.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\record_label_v_cat_2k.csv')
# file = pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\融360_v3back.csv')


# mulddata= pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\multload.csv')
# kv=coll.defaultdict(int)
# for k,v in zip(mulddata['tel'],mulddata['hit']):
#     kv[k]=v if kv[k]<v else kv[k]


# file['mult_load']= [kv[i] if kv[i]>0 else 0 for i in file['tel']  ]
# import sklearn.datasets.load_svmlight_file as ps

'''
data clean
'''

file.drop(['avg_price','avg_cnt'],axis=1,inplace=False)

# drop_cols=[]
# for col in file.columns[3:]:
#     size=len(file[col])
#     black_size=len([i for i in file[col] if i<0.1])
#     if (black_size/(size*1.0))>0.85:
#         drop_cols.append(col)
#
#
# print '---',drop_cols
# file.drop(drop_cols,axis=1,inplace=True)

for i in file.columns[3:]:
    file[i]=file[i].map(lambda x:data_abnormal(x))
    # if 'cnt_ratio' in i or 'price_ratio' in i :
    if 1==1 :
        col=file[i]
        # min, max=col.min(),col.max()+1

        min, max=col.min(),math.log(col.max()+1,2)
        # file[i]=(col - min) / (min - max)
        # file[i]=(col - col.mean()) / col.std()  #zscore

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

# 删除相关性


# 特征交叉
# index_data=feature_cross(index_data)
# test_featrue=feature_cross(test_featrue)

# 划分训练测试集
from xgboost.sklearn import XGBClassifier
from sklearn.preprocessing import Imputer, StandardScaler
features=index_data.columns

# step=2     blag 0.747359870024
ls=[]
feature_kv=coll.defaultdict(int)

from sklearn.decomposition import PCA, SparsePCA,IncrementalPCA


# for step  in [6]:

index_data=Imputer().fit_transform(index_data)




# 丢弃覆盖率低的特征
# from sklearn.feature_selection import VarianceThreshold
# sel = VarianceThreshold(threshold=(.8 * (1 - .8)))
# print  len(index_data.columns)
# val =sel.fit(index_data)
# print index_data.shape

from sklearn.linear_model import (RandomizedLasso, lasso_stability_path,
                                  LassoLarsCV)



for step  in xrange(10):
    print '---------------------',step
    train_X,test_X,train_Y,test_Y=train_test_split(index_data,index_lable ,  test_size=0.25, random_state=step)
    from imblearn.combine import SMOTEENN,SMOTETomek
    from imblearn.over_sampling import *
    sm = SMOTE()
    # sm = SMOTEENN()
    # sm = SMOTETomek()
    # sm = ADASYN()

    print '训练 测试 总共样本' , len(train_X),len(test_X) ,len(train_X)+len(test_X)
    print '训练正样本 测试正样本 总共正样本',sum(train_Y['label']),sum(test_Y['label']),sum(train_Y['label'])+sum(test_Y['label'])
    # train_X = Imputer().fit_transform(train_X)
    # test_X = Imputer().fit_transform(test_X)
    # train_X, train_Y = sm.fit_sample(train_X, train_Y)

    # logisReg = linear_model.LogisticRegression(penalty='l2',solver='liblinear' ,n_jobs=4
    #                                            ,
    #                                            class_weight='balanced' ,max_iter=100, tol=0.0001
    #                                            )

    # alpha_grid, scores_path = lasso_stability_path(train_X, train_Y, random_state=42,
    #                                                eps=0.05)


    logisReg = linear_model.LogisticRegression(penalty='l1',
                                               C=1.0,
                                               solver='liblinear' ,n_jobs=6,
                                               # class_weight={1:0.9, 0:0.1}
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
    top_features=feature_anay(features,f_value,50)
    for k,v  in top_features:
        feature_kv[k]=feature_kv[k]+v
    # print sklearn.feature_selection()._get_feature_importances(lr)
    lr_pred_p= lr.predict_proba(test_X)
    lr_rs=model_rs_dataframe(test_Y,lr_pred_p[:,1])
    lr_auc=metrics.roc_auc_score(test_Y, lr_rs['rs'])
    lr_ks=np.array([ i for  i in ks_calc(lr_rs)['ks']]).max()
    # model=xgb_sk(train_X,train_Y)
    # xgb_pred=model.predict_proba(test_X)[:,1]
    # print feature_anay(index_data.columns,model.feature_importances_)
    # xgb_rs=model_rs_dataframe(test_Y,xgb_pred)
    # xgb_auc=metrics.roc_auc_score(test_Y['label'], xgb_rs['rs'])
    # xgb_ks=np.array([ i for  i in ks_calc(xgb_rs)['ks']]).max()

    print 'lr'   ,lr_auc ,lr_ks
    ls.append([lr_auc, lr_ks])
for i in [k+str(v) for k,v in sorted(feature_kv.items() , key=lambda t:t[-1],reverse=True)]:print i
df= pd.DataFrame(ls)
print df.mean(axis=0)


