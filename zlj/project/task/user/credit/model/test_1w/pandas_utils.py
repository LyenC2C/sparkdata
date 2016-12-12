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
           # print k,v
           ls.append(k)
    return ls


'''
删除空置率过高的特征
'''
def filter_feature_df(df, filter_v):
    t=(df<=0).sum(axis=0)/len(df)
    drop_f_list=filter_feature(t,filter_v)
    print 'drop f yuzhi: ',filter_v
    print 'drop f ratio: ',len(drop_f_list), len(df.columns) ,len(drop_f_list)*1.0/len(df.columns)
    # print 'drop f ',drop_f_list
    df.drop(drop_f_list,axis=1,inplace=True)

'''
删除空置率过高的样本
'''
def filter_feature_df(df, filter_v):
    t=(df<=0).sum(axis=1)/len(df.columns)
    drop_f_list=filter_feature(t,filter_v)
    print 'drop f yuzhi: ',filter_v
    print 'drop f ratio: ',len(drop_f_list), len(df.columns) ,len(drop_f_list)*1.0/len(df.columns)
    # print 'drop f ',drop_f_list
    df.drop(drop_f_list,axis=1,inplace=True)
    return df



'''
丢弃波动太小特征
'''
def std_fiter(df, filter_v):
    ls=[]
    for col in df.columns:
        std=df[col].std()
        if(std<filter_v):
            ls.append(col)
    return ls

'''

  # Get a random DataFrame
    df = pandas.DataFrame(numpy.random.randn(25, 3), columns=['a', 'b', 'c'])

    # Make some random categorical columns
    df['e'] = [random.choice(('Chicago', 'Boston', 'New York')) for i in range(df.shape[0])]
    df['f'] = [random.choice(('Chrome', 'Firefox', 'Opera', "Safari")) for i in range(df.shape[0])]
    print df

    # Vectorize the categorical columns: e & f
    df, _, _ = one_hot_dataframe(df, ['e', 'f'], replace=True)
    print df

'''

from sklearn.feature_extraction import DictVectorizer
def one_hot_dataframe(data, cols, replace=False):
    """ Takes a dataframe and a list of columns that need to be encoded.
        Returns a 3-tuple comprising the data, the vectorized data,
        and the fitted vectorizor.
    """
    vec = DictVectorizer()
    mkdict = lambda row: dict((col, row[col]) for col in cols)
    # manuplate the column
    vecData = pd.DataFrame(vec.fit_transform(data[cols].apply(mkdict, axis=1)).toarray())
    # get column names
    vecData.columns = vec.get_feature_names()
    vecData.index = data.index
    if replace is True:
        data = data.drop(cols, axis=1)
        #column join based on index
        data = data.join(vecData)
    return (data, vecData, vec)
'''
取值然后修改 sex==male 的所有 sex 数据
titanic.loc[titanic["Sex"] == "male", "Sex"] = 1
'''


# 列转行
b_group.pivot(values='count', index='userid', columns='view')