#coding:utf-8
#author:lt

import pandas as pd
import numpy as np
import math

class FeatureInterFace():

    def __init__(self):
        #状态码
        self.status = {0: 'label列或者target_feature列数据缺失',
                       -1: '特征数据中包含非数值数据',
                       -2: '数据不是二分类,无法计算IV值',
                       -3: '输入数据缺失',
                       -4: '离散特征数据分组较多'}

    def calcPearsonCorr(self, feature_data, target_cols=[], label_col='label'):
        '''
        计算特征与目标列皮尔逊相关系数
        :param feature_data: 待计算数据(dataframe)
        :param target_cols: 指定计算相关系数的列(list,不指定则计算所有列)
        :param label_col: 目标列列名(默认为label)
        :return:
        '''
        columns_corr = {}
        # 数据检测
        target_data = feature_data.copy()
        target_data.fillna(value=np.nan) #缺失数据处理
        if target_data[label_col].dropna().unique().size < 2:
            #label 值相同(全为空或其他值),相关系数返回0.0
            return dict.fromkeys(target_cols, 0.0)
        if not target_cols:
            target_cols = target_data.columns
        # 计算指定列与目标列相关系数
        try:
            target_data[label_col] = target_data[label_col].astype(np.float64)
            for col in target_cols:
                target_data[col] = target_data[col].astype(np.float64)
                col_corr = target_data[col].corr(target_data[label_col])
                columns_corr[col] = col_corr
        except Exception as e:
            return -1
        return columns_corr

    def overlapRatio(self,data,result_dic,label_col='label'):
        '''
        覆盖率计算
        :param data: 待计算数据
        :param label_col: 标签列
        :return:
        '''
        total_num = len(data.fillna(value=np.nan))
        overlap_num = len(data.dropna(subset=[label_col]))
        if total_num>0:
            overlapRtio = 1.0*overlap_num/total_num
            result_dic['total_num'] = total_num
            result_dic['overlap_num'] = overlap_num
            result_dic['overlapRtio'] = overlapRtio
        else:
            result_dic['error'] = self.status[0]

    def calcWoeIv(self, data, target_col, col_type, group_num):
        '''
        计算特征的woe,iv值
        :param data: 待计算数据,包含label列和特征列(dataframe)
        :param target_col: 目标列名
        :param col_type: 目标数据类型,连续型continuous,离散型discrete
        :param group_num: 连续型数据分组个数.默认10个,自定义不能超过100个分组
        :return: 返回目标数据每个分组统计信息(dataframe)
        '''
        target_data = data.copy()
        # 将分组信息保存到dataframe中


        #统计正负样本总数,去除特征值为空的行
        target_data.dropna(subset=[target_col], inplace=True)
        if len(target_data) > 0:
            total_count = len(target_data)
            labels = target_data.label.dropna().unique()
            if len(labels) == 1:
                labels.append('-9999')
            else:
                good_total = len(target_data[target_data.label == labels[0]])
                bad_total = len(target_data[target_data.label == labels[1]])
                if good_total == 0: good_total += 1
                if bad_total == 0: bad_total += 1
        else:
            return -3
        #数据分组
        goup_flag = 'group_num'+'_'+target_col
        if col_type =='continuous':
            target_data[goup_flag] = target_data[target_col].rank(method='max')/(total_count/group_num)
            target_data.fillna(-999)
            target_data[goup_flag] = target_data[goup_flag].apply(lambda x: int(x) if x>0 else -1)
        if col_type =='discrete':
            size = target_data[target_col].unique().size
            if size > 100:
                return -4
            target_data[goup_flag]=target_data[target_col]

        #统计每个分组信息
        groups_info = []
        for group_name, group_data in target_data.groupby(by=goup_flag):
            g={}
            g['group_name'] =group_name
            g['group_count'] = len(group_data)
            g['group_ratio'] = 1.0*g['group_count']/total_count
            g['good_count'] = len(group_data[group_data.label == labels[0]])
            g['bad_count'] = len(group_data[group_data.label == labels[1]])
            g['good_ratio'] = 1.0 * g['good_count'] / good_total
            g['bad_ratio'] = 1.0 * g['bad_count'] / bad_total
            if g['good_count'] > 0 and g['bad_count'] > 0:
                g['woe'] = np.math.log(g['good_ratio']/g['bad_ratio'])
            elif g['good_count'] ==0:
                g['woe'] = -1
            else:
                g['woe'] = 1
            g['iv'] = 1.0*(g['good_ratio']-g['bad_ratio']) *g['woe']
            groups_info.append(g)

        df_columns = ['group_name', 'group_count', 'group_ratio', 'good_count', 'good_ratio', 'bad_count',
                      'bad_ratio', 'woe', 'iv']
        group_info_df = pd.DataFrame(groups_info, columns=df_columns)
        return group_info_df

    def calcFeatureIv(self,feature_data,target_cols=[],cols_type=[],group_num=10,iv_limit=10):
        '''
        计算指定数据集的IV值
        :param feature_data: 需要统计的数据(dataframe)
        # :param label_col: 数据标签或类别(list)
        :param target_cols: 指定统计IV值的列名(list,默认计算所有列)
        :param cols_type: 指定统计IV值列的数据类型(list):连续型continuous,离散型discrete
        :param group_num: 指定连续型数据分组个数,默认为10个组
        :param iv_limit: IV值筛选
        :return: 每列IV值(dic)
        '''
        #label列合法性
        columns_iv = {}
        feature_data.fillna(value=np.nan)
        #多分类,无法计算iv值
        if feature_data.label.dropna().unique().size > 2:
            return -2
        #无label信息,特征信息熵为0
        if feature_data.label.dropna().unique().size ==0:
            return dict.fromkeys(target_cols,0.0)
        # 样本特征对应标签只有一类,特征信息熵为0
        if feature_data.label.dropna().unique().size ==1:
            return dict.fromkeys(target_cols,1.0)
        if not target_cols:
            target_cols = feature_data.columns
        #计算每个指定列的IV值
        for col_index in range(len(target_cols)):
            calc_data = feature_data.loc[:,['label',target_cols[col_index]]]
            feature_woe_iv = self.calcWoeIv(calc_data, target_cols[col_index], cols_type[col_index], group_num)
            if isinstance(feature_woe_iv, int):
                return feature_woe_iv
            else:
                print( feature_woe_iv )
                columns_iv[target_cols[col_index]] = feature_woe_iv.iv.sum()
        return columns_iv

    def checkData(self,target_data):
        #数据检测
        target_data.fillna(value=np.nan)
        if 'label' not in target_data.columns or 'target_feature' not in target_data.columns:
            return 0
        if target_data.dropna(how='all').shape[0] == 0:
            return -3
        return 1

    def run(self,data_file, data_type='continuous'):
        '''
        待计算列列名:target_feature,标签列列名:label
        :param data_file: 特征数据文件路径
        :param data_type: 特征列数据类型(默认连续型continuous,离散型discrete)
        '''
        result_item = ['total_num','overlap_num','overlapRtio','pearsonCorr','iv']
        result_dic = dict.fromkeys(result_item, 'null')
        try:
            target_data = pd.read_csv(data_file)
            flag = self.checkData(target_data)
            if flag >0:
                self.overlapRatio(target_data,result_dic)
                pearsonCorr = self.calcPearsonCorr(target_data,target_cols=['target_feature'])
                iv = self.calcFeatureIv(target_data,target_cols=['target_feature'],cols_type=[data_type])
                if isinstance(pearsonCorr,dict):
                    result_dic['pearsonCorr'] = pearsonCorr
                else:
                    result_dic['error'] = self.status[pearsonCorr]
                if isinstance(iv,dict):
                    result_dic['iv'] = iv
                else:
                    result_dic['iv'] = self.status[iv]
            else:
                result_dic['error'] = self.status[flag]
        except Exception as e:
            result_dic['error'] = e
        return result_dic

if __name__=='__main__':
    #测试
    obj = FeatureInterFace()
    data_file_path = './feature_10000.csv'
    result_dic = obj.run(data_file_path, data_type='continuous')
    print result_dic



