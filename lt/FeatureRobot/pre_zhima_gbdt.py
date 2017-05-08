# coding: utf-8

# In[9]:

import pandas as pd
import numpy as np
from svmloader import load_svmfile
import multiprocessing
from multiprocessing import Pool, Lock
from sklearn.model_selection import train_test_split
import copy
import xgboost as xgb
import random
import gc
import re
import os
import pickle
import warnings

warnings.filterwarnings("ignore")
from scipy import sparse
from scipy.sparse import csr_matrix, csr
from scipy import sparse
import os
import commands
import datetime


'''
提前将使用训练数据将模型训练好
之后采用GBDT特征构造模型为所有样本构造新特征
'''

def load(filename):
    # 加载数据
    x, y = load_svmfile(filename)
    return x, y


# In[3]:

def get_tree(model):
    '''
    输入:gbdt造特征模型

    return:得到叶子节点对应index
    '''
    trees = model.get_dump()
    Lst = list()
    for i in range(len(trees)):
        big = dict()
        x = 0
        for line in trees[i].split("\n"):
            k = dict()
            z = dict()
            arr = line.split("[")
            if len(arr) == 1:
                try:
                    # z["leaf"] = int(arr[0].split(":")[0])
                    x = x + 1
                    # z["leafrank"] = x
                    big[int(arr[0].split(":")[0])] = x
                except:
                    continue
                    #             else:
                    #                 k["num"] = int(arr[0].split(":")[0])
                    #                 k["featurename"] = int(arr[1].split()[0].split("<")[0].split("f")[1])
                    #                 #k["featurenametrue"] = head[0,int(arr[1].split()[0].split("<")[0].split("f")[1])]
                    #                 k["splitpoint"] = float(arr[1].split()[0].split("<")[1].split("]")[0])
                    #                 k["yesnext"] = int(arr[1].split("]")[1].split("=")[1].split(",")[0])
                    #                 k["nonext"] = int(arr[1].split("]")[1].split("=")[1].split(",")[0])+1
                    #                 k["missing"] = int(arr[1].split("]")[1].split("=")[3])
                    #                 k["leaf"] = "NONE"
                    #                 big[int(arr[0].split(":")[0])]=k

        Lst.append(big)
    treemaxnum = []
    for i in Lst:
        treemaxnum.append(len(i))
    kkt = []
    x = 0
    for i in treemaxnum:
        if x == 0:
            kkt.append(0)
            kkt.append(i)
        else:
            kkt.append(kkt[-1] + i)
        x = x + 1
    for i in range(len(Lst)):
        for k, v in Lst[i].items():
            Lst[i][k] = v + kkt[i]
            #     prex = pd.DataFrame(pre.copy())
            #     for col in range(pre.shape[1]):
            #         prex.iloc[:,col] = prex.iloc[:,col].map(lambda x:Lst[col][x]["leafrank"])
            #         print "col{} finish".format(col)
    return Lst


# In[4]:

def make_pre(model, x_pre):
    '''
    输入：
    model:gbdt造特征模型
    x_pre:输入的x
    '''
    pre = model.predict(xgb.DMatrix(x_pre), pred_leaf=True)
    return pre


# In[5]:

def tolibsvm(lst, prex, x_before, filename):
    '''
    输入：
    lst:叶子节点对应index
    prex:预测得到的叶子节点
    x_before：原数据
    filename: 输出文件名

    输出：已经与原数据结合得到的新数据
    '''

    libsvmall = []
    tt = 0
    for rows in range(prex.shape[0]):
        t = 0
        for i in np.array(prex[rows, :]):
            if tt == 0:
                libsvm = str(str(rows) + "\t" + str(lst[t][i]) + ":" + "1" + " ")
            elif t == 0:
                libsvm = libsvm + str(str(rows) + "\t" + str(lst[t][i]) + ":" + "1" + " ")
            else:
                libsvm = libsvm + str(str(lst[t][i]) + ":" + "1" + " ")
            t = t + 1
            tt = 1
        libsvm = libsvm + ("\n")
    f = open("../abnew/{}".format(filename), "wb")
    f.write(libsvm)
    f.close()
    x, id = load("../abnew/{}".format(filename))
    x_all = sparse.hstack([x_before, x])
    if x_all.shape[1] != 13505:
        add = csr_matrix(np.zeros([x_all.shape[0], (13505 - x_all.shape[1])]))
        x_all = sparse.hstack([x_all, add])
    os.remove("../abnew/{}".format(filename))
    print "finish tolibsvm"
    print filename, x_all.shape
    return x_all


# In[6]:

def process(filelist, model, model_pre):
    '''
    多进程跑文件

    filelist:文件列表
    model_pre:   预测模型
    model:       造特征模型
    '''

    pool = Pool(processes=12)
    for i in filelist:
        if i != "libsvm":
            result = pool.apply_async(change, (i, model, model_pre,))
    pool.close()
    pool.join()
    if result.successful():
        print 'successful'


def change(i, model, model_pre):
    '''
    :param i: 训练样本文件名
    :param model: GBDT特征构造模型
    :param model_pre: 训练好的预测模型
    :return: null
    '''
    path = " ../t_credit_feature_merge_online/ds=20170107/"
    path_new = '/home/cwl/datas/t_credit_feature_merge_online/ds=20170107/libsvm/'
    print i
    path_in = path + i
    path_tmp = path_new + i + '_new.libsvm'
    cmd = " awk -F '\001'   '{print $1,$2}' %s > %s " % (path_in, path_tmp)
    commands.getoutput(cmd)
    x, id = load(path_tmp)
    pre = make_pre(model, x)
    x_all = tolibsvm(lst, pre, x, str(i + "_cache"))
    preY = model_pre.predict(xgb.DMatrix(x_all))
    result = pd.DataFrame(preY, index=id).astype(int)
    result.to_csv(path_new + "result_{}.csv".format(i), header=None)
    os.remove(path_tmp)


if __init__=="__main__":

    # 加载模型
    model = xgb.Booster()
    model.load_model("../abnew/gbdtmakenew.model")
    model_pre = xgb.Booster()
    model_pre.load_model("../abnew/pre.model")


    # 得到文件列表
    filelist = os.listdir("../t_credit_feature_merge_online/ds=20170107")


    # 开始跑
    process(filelist, model, model_pre)




