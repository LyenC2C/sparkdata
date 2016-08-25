#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')


from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import confusion_matrix, mean_squared_error
from sklearn.cross_validation import train_test_split
import numpy as np
import matplotlib.pyplot as plt
import pandas as  pd
data=[]
# for line in open('D:\\nlp\\t2'):
#     data.append([ i if i.replace('.','').isdigit() else '0' for i in line.strip().split('\001')])

yimei_df=pd.read_excel('D:\\workcode\\ym_model_v2.xlsx')

df = pd.read_csv('D:\\nlp\\query_result (9).csv')
weidu=len(df.columns)


xianyu_df =pd.read_csv('D:\\nlp\\xianyu_info')


# yimei_df=pd.read_csv('D:\\workcode\\yimei.csv')

keys=[
       u't2.consume_price',
       u't2.cat_id_num',
       u't2.root_cat_id_num',
       u't2.brand_id_num',
       u't2.brand_effec_id_num',
       u't2.brand_no_effec_id_num',
       u't2.no_annoy_num',
       u't2.annoy_num',
       u't2.b_bc_type_num',
       u't2.c_bc_type_num',
       u't2.alipay',
       u't2.regtime_month',
       u't2.buycnt',
       u't2.verify_level',
       u't2.user_per_level',
       u't2.ac_score_normal',
       u't2.sum_level',
       u't2.feed_default',
       u't2.feed_non_default'
     # ]
    ,
    #    u't2.birthday',
    # # u'constellation',
    # u't2.gender',
    # u't2.totalCount' ,
    # u't2.cat_flag',
    # u't2.house_flag'
]

model_df=yimei_df[yimei_df['class']=='JC']

# print df.head()
# import  random
# df_1 = df[df['label'=='1']]
# df_0 = df[df['label'=='0']]
# rand_idx = random.choice(df_0.index,1000,replace=False)
# df_0 = df_0.ix[rand_idx]
# model_df = pd.merge(df_1, df_0)
# print df.shape
# print model_df.shape
#
# # print model_df_tmp.shape
# #
# # model_df=model_df_tmp[(model_df_tmp['label']==1)==True | (model_df_tmp['label']==0 & random.random()<0.2)==True]
# print model_df.shape
test_df=yimei_df[yimei_df['class']=='YM']

# df_new=pd.merge(df,xianyu_df,left_on='t2.user_id',right_on='userId')
# df_new=df_new
# y=df_new['t1.id1'].astype(np.float)
data=model_df[keys]
y=model_df['label']

data_test=test_df[keys]
y_test1=test_df['label']



#
#
# yimei_data=yimei_df[keys]
# yimei_lable=yimei_df['a.phone_no']
# # print y
# print len(data),len(y)
X_train, X_test, y_train, y_test = train_test_split(data, y, test_size=0.01, random_state=59)

print type(X_train)

print(len(X_train),len(y_train))


params = {'n_estimators': 1000, 'max_depth': 3, 'min_samples_split': 1,
          'learning_rate': 0.01}
clf = GradientBoostingClassifier(**params)


clf.fit(X_train,y_train)

predictions = clf.predict(data_test)
print(mean_squared_error(y_test1, predictions))


scores=clf.decision_function(data_test)
print scores
phone=test_df['a.phone_no']
for k,v ,t in zip(phone,predictions,scores):
       print k,v,t

# yimei_p=clf.predict(yimei_data)

# for k,v in zip(yimei_lable,yimei_p):
#        print k,v
# print( yimei_p)


feature_importance = clf.feature_importances_
# make importances relative to max importance
# feature_importance = 100.0 * (feature_importance / feature_importance.max())
feature_importance = 100.0 * (feature_importance)
sorted_idx = np.argsort(feature_importance)
# print feature_importance ,sorted_idx



# feature_names=np.array(data.columns)
feature_names=np.array(keys)
# feature_names=np.array([
# 'consume_price        ',
# 'cat_id_num           ',
# 'root_cat_id_num      ',
# 'brand_id_num         ',
# 'brand_effec_id_num   ',
# 'brand_no_effec_id_num',
# 'no_annoy_num         ',
# 'annoy_num            ',
# 'b_bc_type_num        ',
# 'c_bc_type_num        ',
# 'alipay               ',
# 'regtime_month        ',
# 'buycnt               ',
# 'verify_level         ',
# 'dim_size             ',
# 'brand_size           ',
# 'user_per_level       ',
# 'ac_score_normal      ',
# 'sum_level            ',
# 'avg_month_buycnt     ',
#     'feed_default',
#     'feed_no_default'
# ])
pos = np.arange(sorted_idx.shape[0]) + .5
plt.barh(pos, feature_importance[sorted_idx], align='center')
plt.yticks(pos, feature_names[sorted_idx])

plt.xlabel('Relative Importance')
plt.title('Variable Importance')
plt.show()