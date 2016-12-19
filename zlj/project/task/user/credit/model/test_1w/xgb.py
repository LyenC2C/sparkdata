#coding:utf-8

__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from xgboost.sklearn import XGBClassifier
from   sklearn import  metrics
from sklearn.cross_validation import train_test_split
import warnings
warnings.filterwarnings("ignore")
from model_utils import *






record = pd.read_csv(u'E:\\项目\\1-征信&金融\\模型\\rong360\\fix\\record_label_v_cat.csv')


'''
data clean
'''
# print file.columns[3:]
for i in record.columns[3:]:
    record[i]=record[i].map(lambda x:data_abnormal(x))

import  math
for i  in ['total_price','avg_price','std_price']:
    record[i]=record[i].map(lambda x:math.log(x+0.1,2))
print 'len(data):',len(record)

rank_size=10
# rank feature
record.fillna(-1)
# for  col in record.columns[5:]:
#     # v=record[col].map(lambda x:data_abnormal(x))
#     v=record[col]
#     record_size=len(record)
#     record['rn_'+col]=v.rank(method='max')/int(record_size/rank_size)
#     record['rn_'+col].astype(int)
#
# # rank 10等分 计数特征
# for i in xrange(rank_size):
#     j=i+1
#     record['n_'+str(j)]=(record.iloc[:,3:]==j).sum(axis=1)

record['null']=(record.iloc[:,3:]<=-0.5).sum(axis=1)
data=      record[record['class']=='8000_c']
# data=      file
valid_data=record[record['class']=='2000_c']



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
max_depth=6
min_child_weight=3
subsample=1.0
colsample_bytree=0.8
alpha=10
llambda=550
gamma=0.2
round=200
import xgboost as xgb
def xgb_origin(train_X,train_Y):
    params = {'booster':'gblinear','nthread':4,
    "objective": "binary:logistic",  "eta": eta,"max_depth": max_depth,
                 "min_child_weight": min_child_weight, "silent": 0, "subsample": subsample,
                               "colsample_bytree":colsample_bytree,  "seed": 1,"alpha":alpha,"lambda":llambda}
    xgb.train(params,xgb.DMatrix(train_X,label=train_Y) ,num_boost_round=round).predict(xgb.DMatrix(test_X))



def xgb_sk(train_X,train_Y,eval):
    xgbsk=XGBClassifier(max_depth=max_depth, learning_rate=eta,
                 n_estimators=round, silent=True,
                 objective="binary:logistic",
                 nthread=4, gamma=0, min_child_weight=1,
                 max_delta_step=0, subsample=subsample, colsample_bytree=colsample_bytree, colsample_bylevel=1,
                 reg_alpha=alpha, reg_lambda=llambda,
                        scale_pos_weight=3728/314.0,
                        seed=0)
    xgb_model=xgbsk.fit(train_X,train_Y, eval_set=eval ,eval_metric=['auc'], early_stopping_rounds=50)
    return xgb_model

# 特征分析
def feature_anay(features,feature_importances_):
    pair=zip(features,feature_importances_)
    data=sorted(pair,key=lambda t:t[-1],reverse=True)
    return data[:50]

random_seed = 1225
train_X,val_X,train_Y,val_Y=train_test_split(data.iloc[:,3:],data.label ,  test_size=0.25, random_state=1)
dval = xgb.DMatrix(val_X,label=val_Y)
dtrain = xgb.DMatrix(train_X, label=train_Y)
params={
	'booster':'gbtree',
	'objective': 'binary:logistic',
	'early_stopping_rounds':100,
	'scale_pos_weight': 41697.0/5326.0,
        'eval_metric': 'auc',
	'gamma':0.1,#0.2 is ok
    'silent':1 ,
	'max_depth':4,
	'lambda':550,
        'subsample':0.7,
        'colsample_bytree':0.3,
        'min_child_weight':2.5,
        'eta': 0.007,
	'seed':random_seed,
	'nthread':4
    }

watchlist  = [(dtrain,'train'),(dval,'val')]#The early stopping is based on last set in the evallist

model_v6 = xgb.train(params,dtrain,num_boost_round=5000,early_stopping_rounds=500  ,
	# feval=feval_ks_calc,
	evals=watchlist
             )

import matplotlib.pyplot as plt
import pandas as pd
import  seaborn as sns
import matplotlib
matplotlib.style.use('ggplot')
def plot_feature(model_v6,topN=50):
    # %matplotlib inline
    s=pd.DataFrame.from_dict(model_v6.get_fscore(),orient='index')
    s.columns=['weight']
    s.sort_values(by='weight').tail(topN).plot(kind='barh')
    plt.show()
plot_feature(model_v6)
# for step  in xrange(10):
#     print '---------------------',step
#
#     # print list(index_data.columns)
#     train_X,test_X,train_Y,test_Y=train_test_split(index_data,index_lable ,  test_size=0.25 , random_state=step)
#     print len(train_X),len(test_X)
#     print sum(train_Y['label']),sum(test_Y['label'])
#
#     # print xgb.train(params,xgb.DMatrix(train_X,label=train_Y) ,num_boost_round=round).predict(xgb.DMatrix(test_X))
#     # xgb_pred=xgb.train(params,xgb.DMatrix(train_X,label=train_Y) ,num_boost_round=200).predict(xgb.DMatrix(test_X))
#     model=xgb_sk(train_X,train_Y,[(test_X,test_Y)])
#     print  'best best_ntree_limit' ,model.best_ntree_limit
#     xgb_pred=model.predict_proba(test_X,ntree_limit=model.best_ntree_limit)[:,1]
#     print feature_anay(index_data.columns,model.feature_importances_)
#     xgb_rs=model_rs_dataframe(test_Y,xgb_pred)
#     xgb_auc=metrics.roc_auc_score(test_Y['label'], xgb_rs['rs'])
#     xgb_ks=np.array([ i for  i in ks_calc(xgb_rs)['ks']]).max()
#     print  'xgb', xgb_auc,xgb_ks
#     ls.append([xgb_auc,xgb_ks])

# df= pd.DataFrame(ls)
# print df.mean(axis=0)
