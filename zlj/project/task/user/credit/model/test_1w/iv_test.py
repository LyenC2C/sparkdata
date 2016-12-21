#coding:utf-8
from sklearn.decomposition import PCA
from sklearn.utils import column_or_1d


__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')
import os

from pandas_utils import *
import  pandas as pd
'''
iv 值测试
'''
path=u'E:\\项目\\1-征信&金融\\1-融360项目\\data\\'
df=pd.read_csv(path+'xianyu.csv')

import math
def fun(x):
    print x
    return math.log(x)
df['price']=df.price.apply(lambda x:fun(x+1))

calc_feature_woe_iv(df[df.label>-1],'price',col_type='continuous',bin_size=20,verbose=True).to_csv(path+u'price.csv')
# calc_feature_woe_iv(df[df.label>-1],'detailfrom',col_type='discrete',bin_size=20,verbose=True).to_csv(path+u'detailfrom.csv')
# calc_feature_woe_iv(df[df.label>-1],'zhima',col_type='discrete',bin_size=20,verbose=True).to_csv(path+u'zhima.csv')
# calc_feature_woe_iv(df[df.label>-1],'shiren',col_type='discrete',bin_size=20,verbose=True).to_csv(path+u'shiren.csv')