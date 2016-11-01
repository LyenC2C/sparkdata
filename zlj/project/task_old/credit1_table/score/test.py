#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import numpy as np
import matplotlib.pyplot as plt
import pandas as  pd

# df = pd.read_csv('D:\\nlp\\query_result (3).csv')
# weidu=len(df.columns)


yimei_df=pd.read_csv('D:\\workcode\\ym_model.csv')


import random
data= yimei_df[yimei_df['class']=='JC']
print data[(yimei_df['label']==0) &  (random.random()<0.3) ].shape

# xianyu_df =pd.read_csv('D:\\nlp\\xianyu_info')
# print weidu,df.columns
#
# X=df.iloc[:,0:weidu-1]
# df_new=pd.merge(X,xianyu_df,left_on='t_base_credit_consume_join_data_zm_data.user_id',right_on='userId')
# y=df.iloc[:,weidu-1].astype(np.float)
#


