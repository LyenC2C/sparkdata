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
'''
删除空置率过高的特征
'''
def filter_feature(t,score):
    ls=[]
    kv=zip(t.index, t.values)
    for k,v in kv:
       if v>score:
           ls.append(k)
    return ls


'''
删除空置率过高的特征
'''
def filter_feature_df(df, filter_v):
    t=(df<=0).sum(axis=0)/len(file)
    drop_f_list=filter_feature(t,filter_v)
    print 'drop f ratio ',len(drop_f_list), len(drop_f_list)/len(df)
    print 'drop f ',drop_f_list
    df.drop(drop_f_list,axis=1,inplace=True)

'''
丢弃波动太小特征
'''
def std_fiter(df, filter_v):
    return ''