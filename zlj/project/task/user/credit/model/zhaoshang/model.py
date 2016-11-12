#coding:utf-8
__author__ = 'zlj'
import sys
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


def data_process(file_path):
    file = pd.read_csv(file_path)
    data_tag =file[(file['class']=='tag_user') & file['label']==1]
    data_1w=file[file['class']=='test_1w']
    data= pd.concat([data_tag,data_1w])
    return data

data=data_process(u'E:\\项目\\征信&金融\\模型\\test1w\\融360_v3back.csv')


def fun(x):
    # if np.isnan(x):return -1
    try:return float(x)
    except:return -1.0

for i in data.columns[3:]:
    data[i]=data[i].map(lambda x:fun(x))


import  math
data['total_price']=data['total_price'].map(lambda x:math.log(x,2))
print 'len(data):',len(data)

# 特征索引
index_data=data.iloc[:,3:]
features=data.columns[3:]
index_lable= data.loc[:,  ['label']]

index_data.drop([
    '58_fraud_flag',
 'annoy_num',
 'annoy_ratio',
 'b_bc_type_num',
 'car_flag',
 'child_flag',
 'flow_price',
 'house_flag',
 'local_buycount',
 'pet_flag',
 'province_num',
 'qq_gender'
                    # ,'gender','age'
],axis=1,inplace=True)


print index_data.columns




# 划分训练测试集
from sklearn.preprocessing import Imputer, StandardScaler
step=3
stda=StandardScaler()
eta=0.05
max_depth=6
min_child_weight=10
subsample=1.0
colsample_bytree=0.8
alpha=1
llambda=200
gamma=0.2



# step=2     blag 0.747359870024
ls=[]
for step  in [2]:
    print '---------------------',step
    train_X,test_X,train_Y,test_Y=train_test_split(index_data,index_lable ,  test_size=0.3 , random_state=step)
    from imblearn.combine import SMOTEENN,SMOTETomek
    from imblearn.over_sampling import *
    # sm = SMOTE()
    sm = SMOTEENN()
    # sm = SMOTETomek()
    # sm = ADASYN()

    print len(train_X),len(test_X),len(train_Y),sum(train_Y['label']),len(test_Y),sum(test_Y['label'])
    train_X = Imputer().fit_transform(train_X)
    test_X = Imputer().fit_transform(test_X)

    # 模型1 gbdt
    # clf = GradientBoostingClassifier(learning_rate=0.3,  max_depth=4, random_state=59,subsample=1.0,n_estimators=100).\
    #     fit(train_X, train_Y)
    #
    # gbdt_pred= clf.predict(test_X)
    # gbdt_pred_p= clf.predict_proba(test_X)
    #
    #
    # print train_Y.shape, test_Y.shape
    # print metrics.accuracy_score(test_Y,gbdt_pred)
    # roc_test = roc_curve(test_Y, gbdt_pred_p[:,1])
    #
    # plt.plot(roc_test[0], roc_test[1],label=u'sklearn gbdt')

    # 模型2  随机森林

    rf=RandomForestClassifier(n_estimators=100)
    rf.fit( train_X, train_Y)
    rf_pred_p= rf.predict_proba( test_X)
    rf_roc_test = roc_curve(test_Y, rf_pred_p[:,1])
    plt.plot(rf_roc_test[0], rf_roc_test[1],color='c',label=u'sklearn rf')


    from sklearn.feature_selection import SelectFromModel

    # 模型3 LR
    logisReg = linear_model.LogisticRegression(penalty='l2')
    lr =logisReg.fit(train_X,train_Y)
    # 特征筛选
    # df=SelectFromModel(linear_model.LogisticRegression(penalty="l2", C=0.1)).fit(train_X,train_Y)
    # mask=df._get_support_mask()
    # print [v  for k,v in zip(mask,features) if k==True]
    # df=SelectFromModel(linear_model.LogisticRegression(penalty="l1", C=0.1)).fit(train_X,train_Y)
    # mask=df._get_support_mask()
    # print [v for k,v   in zip(mask,features) if k==True]


    lr_pred_p= lr.predict_proba(test_X)

    lr_roc_test = roc_curve(test_Y, lr_pred_p[:,1])
    plt.plot(lr_roc_test[0], lr_roc_test[1],color='r',label=u'sklearn  lr')
    lv=test_Y.values.tolist()

    sys.path.append("..")
    from blagging import BlaggingClassifier

    blag=BlaggingClassifier().fit(train_X,train_Y)
    blag_pred_p=blag.predict_proba(test_X)
    blag_roc_test = roc_curve(test_Y, blag_pred_p[:,1])
    plt.plot(blag_roc_test[0], blag_roc_test[1],color='b',label=u'blag  lr')



    metrics.roc_auc_score(test_Y, BlaggingClassifier().fit(train_X,train_Y).predict_proba(test_X)[:,1])
    metrics.roc_auc_score(test_Y, BlaggingClassifier().fit(train_X,train_Y).predict_proba(test_X)[:,1])


    # 基本线
    base_line=[0.5   for i in xrange(len(test_Y))]
    base_line_roc_test = roc_curve(test_Y, base_line)
    plt.plot(base_line_roc_test[0], base_line_roc_test[1],color='y',label=u'base_line')

    import xgboost as xgb
    params = {'booster':'gbtree','nthread':4,
    "objective": "binary:logistic",  "eta": eta,"max_depth": max_depth,
                 "min_child_weight": min_child_weight, "silent": 1, "subsample": subsample,
                               "colsample_bytree":colsample_bytree,  "seed": 1,"alpha":alpha,"lambda":llambda}
    # print 'gbdt' ,metrics.roc_auc_score(test_Y, gbdt_pred_p[:,1])
    xgb_pred=xgb.train(params,xgb.DMatrix(train_X,label=train_Y) ,num_boost_round=100).predict(xgb.DMatrix(test_X))
    from model_utils import model_rs_dataframe,ks_calc
    from model_utils import model_rs_dataframe,ks_calc
    # gbdt_rs=model_rs_dataframe(test_Y,gbdt_pred_p[:,1])
    lr_rs=model_rs_dataframe(test_Y,lr_pred_p[:,1])
    rf_rs=model_rs_dataframe(test_Y,rf_pred_p[:,1])
    blag_rs=model_rs_dataframe(test_Y,blag_pred_p[:,1])
    xgb_rs=model_rs_dataframe(test_Y,xgb_pred)

    lr_auc=metrics.roc_auc_score(test_Y, lr_rs['rs'])
    rf_auc=metrics.roc_auc_score(test_Y, rf_rs['rs'])
    blag_auc=metrics.roc_auc_score(test_Y, blag_rs['rs'])
    xgb_auc=metrics.roc_auc_score(test_Y['label'], xgb_rs['rs'])

    lr_ks=np.array([ i for  i in ks_calc(lr_rs)['ks']]).max()
    rf_ks=np.array([ i for  i in ks_calc(rf_rs)['ks']]).max()
    blag_ks=np.array([ i for  i in ks_calc(blag_rs)['ks']]).max()
    xgb_ks=np.array([ i for  i in ks_calc(xgb_rs)['ks']]).max()
    print 'lr'   ,lr_auc ,lr_ks
    print 'rf'   ,rf_auc,rf_ks
    print 'blag' ,blag_auc,blag_ks
    print  'xgb', xgb_auc,xgb_ks
    ls.append([lr_auc,rf_auc,blag_auc,xgb_auc ,lr_ks,rf_ks,blag_ks,xgb_ks])

df= pd.DataFrame(ls)
print df.mean(axis=0)