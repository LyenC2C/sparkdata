#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')


import pandas as pd

#  统计
# t=file[file['label']==0]
# t_sum=(t<=0).sum(axis=1)
# df=pd.DataFrame({'k':t_sum,'v':[1 for i in xrange(len(t_sum))]})
# df['v'].groupby[df['k']]

# Series

def filter_feature(t,score):
    ls=[]
    kv=zip(t.index, t.values)
    for k,v in kv:
       if v>score:
           ls.append(k)
    return ls