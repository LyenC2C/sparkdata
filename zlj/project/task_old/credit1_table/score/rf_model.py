#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')


from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import confusion_matrix, mean_squared_error
from sklearn.cross_validation import train_test_split
import numpy as np
import matplotlib.pyplot as plt
import pandas as  pd
data=[]
# for line in open('D:\\nlp\\t_base_credit_consume_join_data_zm_data'):
#     data.append([ i if i.replace('.','').isdigit() else '0' for i in line.strip().split('\001')])


df = pd.read_csv('D:\\nlp\\query_result (3).csv')
weidu=len(df.columns)

#print weidu
data=df.iloc[:,1:weidu-1].as_matrix().astype(np.float)
y=df.iloc[:,weidu-1].astype(np.float)
X_train, X_test, y_train, y_test = train_test_split(data, y, test_size=0.2, random_state=33)

print(len(X_train),len(y_train))


clf=RandomForestRegressor(n_estimators=1000,random_state=33,max_features="sqrt")
clf.fit(X_train,y_train)

predictions = clf.predict(X_test)
print(mean_squared_error(y_test, predictions))

feature_importance = clf.feature_importances_
# make importances relative to max importance
# feature_importance = 100.0 * (feature_importance / feature_importance.max())
feature_importance = 100.0 * (feature_importance )
sorted_idx = np.argsort(feature_importance)
print sorted_idx




feature_names=np.array([
'consume_price        ',
'cat_id_num           ',
'root_cat_id_num      ',
'brand_id_num         ',
'brand_effec_id_num   ',
'brand_no_effec_id_num',
'no_annoy_num         ',
'annoy_num            ',
'b_bc_type_num        ',
'c_bc_type_num        ',
'alipay               ',
'regtime_month        ',
'buycnt               ',
'verify_level         ',
'dim_size             ',
'brand_size           ',
'user_per_level       ',
'ac_score_normal      ',
'sum_level            ',
'avg_month_buycnt     ',
    'feed_default',
    'feed_no_default'
])
pos = np.arange(sorted_idx.shape[0]) + .5
plt.barh(pos, feature_importance[sorted_idx], align='center')
plt.yticks(pos, feature_names[sorted_idx])

plt.xlabel('Relative Importance')
plt.title('Variable Importance')
plt.show()