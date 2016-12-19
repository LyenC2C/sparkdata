#coding:utf-8
from sklearn.decomposition import PCA
from sklearn.utils import column_or_1d
# from zlj.project.task.user.credit.model.test_1w.model_utils import data_abnormal

__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import pandas as pd
import numpy as np

def calc_val_woe_iv(data, tgt_col_name, col_type='continuous' ,bin_size=10,verbose=False):
    """
    根据用户自定义分组来统计good/bad个数，已经对应的WOE，IV值,版本2
    :param dataset: 进行分组计算的数据集名称
    :param grp_var_name: 分组变量名
    :param tgt_var_name: 目标变量名
    :param verbose: 输出中间过程
    :return:
        group_name: 变量分组
        count: 组内样本数量
        total_dist: 组内样本数量/数据集样本数量
        good: 组内tgt=0的样本数量
        good_dist: 组内tgt=0的样本数量/tgt=0的样本数量
        bad: 组内tgt=1的样本数量
        bad_dist: 组内tgt=1的样本数量/tgt=1的样本数量
        bad_rate: bad/count
        woe: ln(good_dist/bad_dist)
        iv: (good_dist - bad_dist)*woe
        total_iv: sum(iv)
    continuous 类型分箱处理
    discrete  按照类型处理
    """
    #check
    if 'label' not in data.columns :
        print 'not label in'
        return ''
    # 计算总体样本量，good/bad总数
    dataset=data.loc[:,['label',tgt_col_name]].copy()
    dataset.dropna(inplace=True)
    total_good  = len(dataset[dataset.label==0])
    total_bad   = len(dataset[dataset.label== 1])
    total_count = len(dataset)
    rank_col='rn_'+tgt_col_name
    if col_type=='continuous':
        dataset[rank_col]=dataset[tgt_col_name].rank(method='max')/(total_count/bin_size)
        dataset[rank_col]=dataset[rank_col].astype(int)
    if col_type=='discrete':
        size=dataset.tgt_col_name.unique().size
        if size>100:
            print 'discrete is too much. size is {}'.format(size)
        dataset[rank_col]=dataset[tgt_col_name]
    # 记录每一个分组的统计结果
    grouping_data = []
    for grp_var, grp_data in dataset.groupby(rank_col):
        d = dict()
        d['group_name'] = grp_var
        d['count'] = len(grp_data)
        d['total_dist'] = 1.0 * d['count'] / total_count
        d['good'] = len(grp_data[grp_data.label == 0])
        d['good_dist'] = 1.0 * d['good'] / total_good
        d['bad'] = len(grp_data[grp_data.label == 1])
        d['bad_dist'] = 1.0 * d['bad'] / total_bad
        d['bad_rate'] = 1.0 * d['bad'] / d['count']
        if d['bad'] > 0 and d['good'] > 0:
            d['woe'] = np.math.log(1.0 * d['good_dist'] / d['bad_dist'])
        elif d["good"] == 0:
            d['woe'] = -1
        else:
            d["woe"] = 1
        d['iv'] = 1.0 * (d['good_dist'] - d['bad_dist']) * d['woe']
        grouping_data.append(d)

    # 保存到数据集
    grouping_cols = ['group_name', 'count', 'total_dist',
                     'good', 'good_dist', 'bad', 'bad_dist',
                     'bad_rate', 'woe', 'iv']
    grouping_df = pd.DataFrame(grouping_data, columns=grouping_cols)
    total_iv = grouping_df.iv.sum()
    if verbose:
        print 'total_iv', total_iv
    return grouping_df
