# #coding:utf-8
# __author__ = 'zlj'
# import sys
#
# reload(sys)
# sys.setdefaultencoding('utf8')
#
# data_filter_rn=data[data['rn']>114]
# train=data_filter_rn.iloc[:,2:]
# train=train.fillna(-1)
# label=data_filter_rn['label']
#
# t=(train<=-0.5).sum(axis=0)*1.0/size
# kv={k:v for k,v in zip(t.index, t.values)}
#
# train_v2=train.copy()
# drop_cols=[k for k,v in kv.iteritems() if v>0.99 and '=' not in k]
# train_v2.drop(drop_cols,axis=1,inplace=True)
# cols=train_v2.columns
# rank_size=20
# size=int(label.count()/rank_size)
# for index,col in  enumerate(cols):
#     if 'view_c' in col or 'bankid' in col or 'label_' in col :
#         print index,col
#         train_v2['rn_'+col]=train_v2[col].rank(method='max')/size
#         train_v2['rn_'+col]
# for i in xrange(rank_size):
#     j=i+1
#     train_v2['n_'+str(j)]=(train_v2==j).sum(axis=1)
# train_v2['null']=(train_v2<=-0.5).sum(axis=1)
# #filter
#
# train_v2.drop('time',axis=1,inplace=True)
#
# train_X,val_X,train_Y,val_Y=train_test_split(train_v2,label ,  test_size=0.25, random_state=1)
# dval = xgb.DMatrix(val_X,label=val_Y)
# dtrain = xgb.DMatrix(train_X, label=train_Y)
# params={
# 	'booster':'gbtree',
# 	'objective': 'binary:logistic',
# 	'early_stopping_rounds':1000,
# 	'scale_pos_weight': (48413.0*1.5)/7183.0,
#     'eval_metric': 'auc',
#     'silent':1 ,
# 	'gamma':0.2,#0.2 is ok
# 	'max_depth':5,
# 	'alpha':1 ,
# 	'lambda':550,
#         'subsample':0.7,
#         'colsample_bytree':0.3,
#         'min_child_weight':2.5,
#         'eta': 0.007,
# 	'seed':random_seed,
# 	'nthread':16
#     }
#
# watchlist  = [(dtrain,'train'),(dval,'val')]#The early stopping is based on last set in the evallist
# evals_result={}
#
# model_v6 = xgb.train(params,dtrain,num_boost_round=10000,early_stopping_rounds=500  ,
# 	# feval=feval_ks_calc,
# 	evals=watchlist,evals_result=evals_result)
# model_n=model_v6
# test_y = model_n.predict(xgb.DMatrix(val_X),ntree_limit=model_n.best_ntree_limit)
# auc=metrics.roc_auc_score(val_Y,test_y )
# ks=evel_ks_calc(test_y,val_Y)
# auc,ks
#
# def feval_ks_calc(preds, dtrain):
# 	import pandas as pd
# 	data=pd.DataFrame({'label':dtrain.get_label(),'rs':preds})
#     data=data.sort_values(by=['rs'])
#     data['sortindex'] =[i for i in xrange(len(data))]
#     data['bucket']= pd.cut(data.sortindex ,10,precision=1)
#     group=data.groupby('bucket',as_index=False)
#     data['bad']=data['label'].map(lambda x: 1 if x==0 else 0)
#     data['good']=data['label'].map(lambda x: 1 if x==1 else 0)
#     cumsum=group.sum().cumsum()
#     kv=group.sum()
#     cumsum['bad_cumsum_p']=cumsum['bad']*1.0/(kv.sum()['bad']+0.1)
#     cumsum['good_cumsum_p']=cumsum['good']*1.0/(kv.sum()['good']+0.1)
#     cumsum['ks']= cumsum['bad_cumsum_p']-cumsum['good_cumsum_p']
#     ks=np.array([ i for  i in cumsum['ks']]).max()
#     auc=metrics.roc_auc_score(dtrain.get_label(),preds )
#     rs=int(auc*1000)+ks
#     return 'ks',1-ks