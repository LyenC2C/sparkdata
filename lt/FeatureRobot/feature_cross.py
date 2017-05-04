# coding: utf-8
__author__="cwl"
# # 主函数

# 加载包
import minepy
import pandas as pd
import numpy as np
from numpy import mat
from multiprocessing import Pool, Lock
import pickle
from sklearn.model_selection import train_test_split
import xgboost as xgb
import random
from sklearn.model_selection import GridSearchCV
import gc
import os
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from sklearn.metrics import roc_curve, auc
import matplotlib.pyplot as plt
import matplotlib as mpl
mpl.style.use('ggplot')
import warnings
warnings.filterwarnings("ignore")


# 自定义switch语句
class switch(object):
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        """Return the match method once, then stop"""
        yield self.match
        raise StopIteration

    def match(self, *args):
        """Indicate whether or not to enter a case suite"""
        if self.fall or not args:
            return True
        elif self.value in args:  # changed for v1.5, see below
            self.fall = True
            return True
        else:
            return False


# 需要调用的函数
def FeatureRobot(X, y, header=[], funs=['+', '-', '*', '/'], col_type=[], domain=[[]],
                 corrtype='pearson', feature2labelcorr=0.01, f2fcorr=0.95,
                 nullnum=0.9, stdnum=0, block=4, process_num=8, modeltype="lin", xgbnum=10,
                 lastchoose='model', IVlabelthre=600, topk=200, importance_type="gain", path=os.path.abspath('.')):
    '''
#         parameters:
#         --------------
#   X,y:                  np.matrix，传入的feature和label
#   header:               X的列名，defaut=[range(X.shape[0])]
#   funs :                运算符的集合，+, -, *, /, 1/x,1/x+1/y,x^2+y^2,log,fft,default = ['+','-']
#   col_type:             list，离散变量的列号 default = []
#   domain:               list，不同领域的列名放在一个list中 default = [[]]
#   corrtype:            相关性系数选择，'MIC','pearson','kendall','spearman','IV' ,default = 'pearson'
#   feature2labelcorr:   特征与label的相关系数大小，超过该值便保留。（0,1] default = 0.01
#   nullnum:             空值率，小于该值便保留 (0,1],    defult = 0.9
#   stdnum:              方差,大于该值便保留    (0,∞),   default = 0
#   block:               将原数据分块的数目，影响速度,    default = 4
#   process_num:          进程数，控制进程数量，越大使用的进程数越多，default=4
#   modelnum:            取该数个模型对特征进行筛选然后取平均,default = 10
#   lastchoose:          'model','IV'
#   IVlabelthre：        利用IV来排序的时候，传入label时，label值如果大于该值就把label赋为1，否则赋为0
#   topk：               选变换特征中前多少个进入模型
#   path:                缓存的存储路径
#   modeltype:           分类模型还是回归模型，分类为log，回归为lin
#   xgbnum:              筛选模型的数量
#   importance_type:       特征排名的方式，["weight","gain","cover"],default="gain"
    '''


    # 将列名放在每一列的第一列,如果header为空,列名默认为列序号
    if header == []:
        header = np.mat([range(X.shape[1])])
    X = np.vstack((header, X))

    global pathpickle
    os.makedirs(path + '/pickleresult')
    pathpickle = path + '/pickleresult'
    if (((float(feature2labelcorr)) and (float(nullnum)) and (float(block)) and (float(process_num)))):
        print "===============================开始进行特征变换，请稍后......===================================="
        X_change = 0
        if (len(domain) > 1):  # 有领域分类,
            for i in range(len(domain)):
                for j in range(i + 1, len(domain)):
                    for op in funs:  # 每一个运算符进行一个全局变换
                        feature_cross(X[:, domain[i]], X[:, domain[j]],
                                      y, op, corrtype, feature2labelcorr,
                                      nullnum, stdnum, block, process_num,
                                      col_type)

        else:  # 无领域分类
            for op in funs:  # 每一个运算符进行一个全局变换
                feature_cross(X, X, y, op, corrtype,
                              feature2labelcorr, nullnum,
                              stdnum, block, process_num, col_type)
        # print "===============================特征变换完成，选出{}个特征====================================".format(att.shape[1])
        # 进行特征相互相关性高的排除
        print "===============================特征变换完成，正在从缓存中载入变换特征，请稍后......===================================="
        # 进行进一步选择筛选，模型或者IV或者结合
        fileList = []
        path = path + '/pickleresult'
        iue = 0
        os.listdir(path)
        files = os.listdir(path)
        for f in files:
            fileList.append(f)
        ff = len(fileList)
        while (True):
            if ff >= 30:
                for i in range(iue * 30, (1 + iue) * 30):
                    a = file(path + '/' + fileList[i], 'rb')
                    try:
                        arr = np.hstack((arr, pickle.load(a)))
                    except:
                        arr = pickle.load(a)
                    a.close()
                    os.remove(path + '/' + fileList[i])
            else:
                for i in range(iue * 30, len(fileList)):
                    a = file(path + '/' + fileList[i], 'rb')
                    try:
                        arr = np.hstack((arr, pickle.load(a)))
                    except:
                        arr = pickle.load(a)
                    a.close()
                    os.remove(path + '/' + fileList[i])
            print "=========================第{}次加载数据，共有变换后特征{}个===========================================".format(
                iue + 1, arr.shape[1])
            if lastchoose == 'model':
                if modeltype == "lin":
                    arr = modelchooselin(arr, y, xgbnum)
                elif modeltype == "log":
                    arr = modelchooselog(arr, y, xgbnum)
                print "=========================模型平均特征排序筛选第{}次完成,特征已经排序,保留前{}个===========================================".format(
                    iue + 1, arr.shape[1])

            elif lastchoose == 'IV':
                if modeltype == "lin":
                    print "线性回归不能使用IV值，已经开始进行模型排序："
                    arr = modelchooselin(arr, y, xgbnum)
                    print "=========================模型平均特征排序筛选第{}次完成,特征已经排序,保留前{}个===========================================".format(
                        iue + 1, arr.shape[1])
                elif modeltype == "log":
                    label = y.copy()
                    arr = forIV(arr, label, IVlabelthre)
                    print "=========================IV特征排序筛选第{}次完成，特征已经排序，保留了前{}个===========================================".format(
                        iue + 1, arr.shape[1])
            iue = iue + 1
            ff = ff - 30
            if ff <= 0:
                break
        os.rmdir(path)
        gc.collect()
        print "==========================特征正在相互博弈，相似特征将被排除，大概需要2分钟，请稍后......==================================="
        X_4 = f2fremove(arr, block=20, process_num=10, f2fcorr=f2fcorr)
        del arr
        print "==========================特征正在与原数据进行博弈，相似特征将被排除，大概需要4分钟，请稍后......==================================="
        X_4t = f2before(X_4, X.copy(), block=20, process_num=10, f2beforecorr=f2fcorr)
        del X_4
        gc.collect()
        s = np.nan_to_num(np.mat(X_4t[1:, :], dtype=float))
        exc = LDA(n_components=10)
        exc.fit(s, y)
        t = exc.transform(s)
        for tnum in range(t.shape[1]):
            if tnum == 0:
                headt = np.mat(["LDA0"])
            else:
                headt = np.hstack((headt, np.mat(["LDA{}".format(tnum)])))
        t = np.vstack((headt, t))
        X_4t = np.hstack((X_4t, t))

        print "==========================特征相关性排除完成,加入LDA特征之后，剩余{}个特征====================================".format(
            X_4t.shape[1])
        #             print X_4[0,:]
        #             print X[0,:]
        print "==========================最后一次特征排序，请稍后......==================================="
        if modeltype == "lin":
            X_5 = modelchooselin(X_4t.copy(), y.copy(), xgbnum)
        elif modeltype == "log":
            X_5 = modelchooselog(X_4t.copy(), y.copy(), xgbnum)
        del X_4t
        gc.collect()
        print "==========================模型正在自动化挑选最佳参数，请稍后......===================================="
        scorefinal, model_final, y_score = model_FINAL(X_5, X, y, topk, modeltype)
        scorestart, model_start, y_ = model_START(X, y, modeltype)
        scorestart = 0.7
        if modeltype == "lin":
            print "==========================模型自动化调参完成，效果提升%.2f%%====================================" % (
            100 * (scorestart - scorefinal) / scorefinal)
        elif modeltype == "log":
            print "==========================模型自动化调参完成，效果提升%.2f%%====================================" % (
            100 * (scorefinal - scorestart) / scorestart)
        print "是否继续深度优化模型？输入“yes”继续，“no”退出"
        panduan = raw_input()
        if X_5.shape[1] < topk:
            topk = X_5.shape[1]
        while ((panduan == "yes") or (panduan == "y") or (panduan == "Y") or (panduan == "Yes")):
            if (topk >= 100):
                topk = topk - 50
            elif (topk >= 50):
                topk = topk - 30
            elif (topk >= 20):
                topk = topk - 10
            elif (topk >= 10):
                topk = topk - 5
            elif (topk >= 5):
                topk = topk - 2
            else:
                topk = topk - 1
            if topk <= 0:
                print "模型需要黑科技的优化！"
                break
            print "开始深度优化模型,请稍后......"
            print "选入特征，数量为{}".format(topk)
            scorefinal, model_final, y_score = model_FINAL(X_5, X, y, topk, modeltype)
            if modeltype == "lin":
                print "==========================模型自动化调参完成，效果提升%.2f%%====================================" % (
                100 * (scorestart - scorefinal) / scorefinal)
            elif modeltype == "log":
                print "==========================模型自动化调参完成，效果提升%.2f%%====================================" % (
                100 * (scorefinal - scorestart) / scorestart)
            print "是否继续深度优化模型？输入“yes”继续，“no”退出"
            panduan = raw_input()

        print "是否需要深度学习特征进行优化模型？输入“yes”继续，“no”退出"
        panduan = raw_input()

        gbdtnameall = 0

        if ((panduan == "yes") or (panduan == "y") or (panduan == "Y") or (panduan == "Yes")):
            Xy, gbdtnameall = gbdtmake(X, y.copy(), modeltype=modeltype)
            scorefinal, model_final, y_score = model_START(Xy[:, :-1], Xy[1:, -1], modeltype, num_boost_round=1000)
            if modeltype == "lin":
                print "==========================模型自动化调参完成，效果提升%.2f%%====================================" % (
                100 * (scorestart - scorefinal) / scorefinal)
            elif modeltype == "log":
                print "==========================模型自动化调参完成，效果提升%.2f%%====================================" % (
                100 * (scorefinal - scorestart) / scorestart)
        label1 = y.copy()
        ss = see(model_final, 15, y_score, label1, gbdtnameall)
        # ss.feature_importance()
        # if modeltype == "log":
        # ss.ks()
        # ss.rocplot()

        print "再见老大，我去休息了"
        return ss



    else:
        print "wrong parameters!"


# # 模型评估,自动化调参

# In[2]:

def model_FINAL(X, feature, label, topk=200, modeltype="lin", importance_type="gain"):
    '''
    测试加入变换后的特征之后模型如何
#         parameters:
#         --------------
#   X:                    np.matrix，传入的特征，第一列为列名
#   feature :             原数据,np.matrix，传入的特征，第一列为列名
#   label :               np.matrix标签
#   topk:                 选传入特征中前多少个数据
#   modeltype:            回归模型或者分类模型
    '''
    if modeltype == "lin":
        params = {
            'booster': 'gbtree',
            'objective': 'reg:linear',
            # 'scale_pos_weight': float(len(y)-sum(y))/float(sum(y)),
            'eval_metric': 'mae',
            'gamma': 0.2,
            'silent': False,
            'max_depth': 7,
            'lambda': 10,
            'reg_alpha': 0,
            'colsample_bytree': 0.3,
            'min_child_weight': 2.5,
            'eta': 0.015,
            'nthread': 6
        }
        data1 = np.mat(X[1:, :topk], dtype=float)
        head1 = list(np.array(X[0, :topk])[0])
        #     print 'head1:',len(head1)
        #     print 'data1:',data1.shape
        head2 = list(np.array(feature[0, :])[0])
        data2 = np.mat(feature[1:, :], dtype=float)
        #     print 'head2:',len(head2)
        #     print 'data2:',data2.shape
        datamore = pd.DataFrame(data1, columns=head1)
        datamore2 = pd.DataFrame(data2, columns=head2)
        #     print datamore.shape
        #     print datamore2.shape
        train_test_X = pd.concat([datamore2, datamore], axis=1)
        #     print train_test_X.shape
        train_test_Y = pd.Series(np.array(label.T)[0])
        #     print train_test_Y.shape


        #         gsearch1 = GridSearchCV(estimator = xgb.XGBRegressor(params),
        #                                 param_grid={'max_depth':range(1,10),
        #                                             'min_child_weight':[i/2.0 for i in range(1,10)]})
        # #     print params
        #         gsearch1.fit(train_test_X,train_test_Y)
        #         params['max_depth'] = gsearch1.best_params_['max_depth']
        #         params['min_child_weight'] = (gsearch1.best_params_['min_child_weight'])
        #         print "max_depth and min_child_weight are ok"
        #         gsearch2 = GridSearchCV(estimator = xgb.XGBRegressor(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight']),
        #                                 param_grid={'gamma':[i/10.0 for i in range(10)]})
        #         gsearch2.fit(train_test_X,train_test_Y)
        #         params['gamma'] = float(gsearch2.best_params_['gamma'])
        #         print "gamma is ok"
        #         gsearch3 = GridSearchCV(estimator = xgb.XGBRegressor(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight'],
        #                 gamma = gsearch2.best_params_['gamma']),
        #                                 param_grid={'subsample':[i/10.0 for i in range(1,10)],
        #                                             'colsample_bytree':[i/10.0 for i in range(2,10)]})
        #         gsearch3.fit(train_test_X,train_test_Y)
        #         params['subsample'] = gsearch3.best_params_['subsample']
        #         params['colsample_bytree'] = gsearch3.best_params_['colsample_bytree']
        #         print "subsample and colsample_bytree are ok"
        #         gsearch4 = GridSearchCV(estimator = xgb.XGBRegressor(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight'],
        #                 gamma = gsearch2.best_params_['gamma'],
        #                 subsample = gsearch3.best_params_['subsample'],
        #                 colsample_bytree = gsearch3.best_params_['colsample_bytree']),
        #                                 param_grid={'reg_alpha':[1e-5,1e-2,0.1,1,100]})
        #         gsearch4.fit(train_test_X,train_test_Y)
        #         params['reg_alpha'] = float(gsearch4.best_params_['reg_alpha'])
        #         print "reg_alpha is ok"

        #         gsearch5 = GridSearchCV(estimator = xgb.XGBRegressor(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight'],
        #                 gamma = gsearch2.best_params_['gamma'],
        #                 subsample = gsearch3.best_params_['subsample'],
        #                 colsample_bytree = gsearch3.best_params_['colsample_bytree'],
        #                 reg_alpha = gsearch4.best_params_['reg_alpha']),
        #                                 param_grid={'lin_lambda':[1e-5,1e-2,0.1,1,100]})
        #         gsearch5.fit(train_test_X,train_test_Y)
        #         params['lin_lambda'] = float(gsearch4.best_params_['lin_lambda'])
        #         print "lin_lambda is ok"
        #         print params


        train_X, val_X, train_Y, val_Y = train_test_split(train_test_X, train_test_Y,
                                                          test_size=0.2, random_state=3)
        dval = xgb.DMatrix(val_X, label=val_Y)
        dtrain = xgb.DMatrix(train_X, label=train_Y)
        watchlist = [(dtrain, 'train'), (dval, 'val')]
        evals_result = {}
        model_final = xgb.train(params, dtrain, num_boost_round=10000, early_stopping_rounds=100,
                                # feval=feval_ks_calc,
                                evals=watchlist,
                                verbose_eval=200,
                                evals_result=evals_result
                                )

        s = pd.DataFrame.from_dict(model_final.get_score(importance_type=importance_type), orient='index')
        s.columns = ['weight']
        f_top500 = s.sort_values(by='weight', ascending=False).head(500)
        df = train_test_X
        Xscore = df.loc[:, list(f_top500.index)]
        XX = xgb.DMatrix(train_test_X)
        y_score = model_final.predict(XX)
        return min(evals_result['val']['mae']), model_final, y_score


    elif modeltype == "log":
        params = {
            'booster': 'gbtree',
            'objective': 'binary:logistic',
            'scale_pos_weight': 484130 / 7183,
            'eval_metric': 'auc',
            'gamma': 0.2,
            'silent': 1,
            'max_depth': 4,
            'lambda': 1000,
            'subsample': 0.7,
            'colsample_bytree': 0.3,
            'min_child_weight': 2.5,
            'eta': 0.016,
            'nthread': 12
        }
        data1 = np.mat(X[1:, :topk], dtype=float)
        head1 = list(np.array(X[0, :topk])[0])

        head2 = list(np.array(feature[0, :])[0])
        data2 = np.mat(feature[1:, :], dtype=float)

        datamore = pd.DataFrame(data1, columns=head1)
        datamore2 = pd.DataFrame(data2, columns=head2)

        train_test_X = pd.concat([datamore2, datamore], axis=1)

        train_test_Y = pd.Series(np.array(label.T)[0])

        #         gsearch1 = GridSearchCV(estimator = xgb.XGBClassifier(params),
        #                                 param_grid={'max_depth':range(1,10),
        #                                             'min_child_weight':[i/2.0 for i in range(1,10)]})
        #     #     print params
        #         gsearch1.fit(train_test_X,train_test_Y)
        #         params['max_depth'] = gsearch1.best_params_['max_depth']
        #         params['min_child_weight'] = (gsearch1.best_params_['min_child_weight'])
        #         print "max_depth and min_child_weight are ok"

        #         gsearch2 = GridSearchCV(estimator = xgb.XGBClassifier(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight']),
        #                                 param_grid={'gamma':[i/10.0 for i in range(10)]})
        #         gsearch2.fit(train_test_X,train_test_Y)
        #         params['gamma'] = float(gsearch2.best_params_['gamma'])
        #         print "gamma is ok"

        #         gsearch3 = GridSearchCV(estimator = xgb.XGBClassifier(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight'],
        #                 gamma = gsearch2.best_params_['gamma']),
        #                                 param_grid={'subsample':[i/10.0 for i in range(1,10)],
        #                                             'colsample_bytree':[i/10.0 for i in range(2,10)]})
        #         gsearch3.fit(train_test_X,train_test_Y)
        #         params['subsample'] = gsearch3.best_params_['subsample']
        #         params['colsample_bytree'] = gsearch3.best_params_['colsample_bytree']
        #         print "subsample and colsample_bytree are ok"


        #     print params


        train_X, val_X, train_Y, val_Y = train_test_split(train_test_X, train_test_Y,
                                                          test_size=0.2, random_state=3)
        dval = xgb.DMatrix(val_X, label=val_Y)
        dtrain = xgb.DMatrix(train_X, label=train_Y)
        evals_result = {}
        watchlist = [(dtrain, 'train'), (dval, 'val')]
        model_1 = xgb.train(params, dtrain, num_boost_round=10000, early_stopping_rounds=100,
                            # feval=feval_ks_calc,
                            verbose_eval=50,
                            evals=watchlist, evals_result=evals_result)

        s = pd.DataFrame.from_dict(model_1.get_score(importance_type=importance_type), orient='index')
        s.columns = ['weight']
        f_top500 = s.sort_values(by='weight', ascending=False).head(500)
        df = train_test_X
        Xscore = df.loc[:, list(f_top500.index)]
        XX = xgb.DMatrix(train_test_X)
        y_score = model_1.predict(XX)
        return max(evals_result['val']['auc']), model_1, y_score


# In[3]:

def model_START(X, label, modeltype, num_boost_round=10000, importance_type="gain"):
    '''
    测试原模型如何
#         parameters:
#         --------------
#   X:                    np.matrix，传入的特征，第一列为列名
#   header:               原数据的列名,list
#   label :               np.matrix标签

    '''
    if modeltype == "lin":
        params = {
            'booster': 'gbtree',
            'objective': 'reg:linear',
            # 'scale_pos_weight': float(len(y)-sum(y))/float(sum(y)),
            'eval_metric': 'mae',
            'gamma': 0.2,
            'silent': False,
            'max_depth': 7,
            'lambda': 10,
            'reg_alpha': 0,
            'colsample_bytree': 0.3,
            'min_child_weight': 2.5,
            'eta': 0.015,
            'nthread': 18
        }
        data1 = np.mat(X[1:, :], dtype=float)
        head1 = X[0, :]
        head1 = list(np.array(head1)[0])
        datamore = pd.DataFrame(data1, columns=head1)

        #     print datamore.shape
        #     print datamore2.shape
        train_test_X = datamore
        train_test_Y = pd.Series(np.array(label.T)[0])

        #         gsearch1 = GridSearchCV(estimator = xgb.XGBRegressor(params),
        #                                 param_grid={'max_depth':range(1,10),
        #                                             'min_child_weight':[i/2.0 for i in range(1,10)]})
        # #     print params
        #         gsearch1.fit(train_test_X,train_test_Y)
        #         params['max_depth'] = gsearch1.best_params_['max_depth']
        #         params['min_child_weight'] = (gsearch1.best_params_['min_child_weight'])
        #         print "max_depth and min_child_weight are ok"
        #         gsearch2 = GridSearchCV(estimator = xgb.XGBRegressor(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight']),
        #                                 param_grid={'gamma':[i/10.0 for i in range(10)]})
        #         gsearch2.fit(train_test_X,train_test_Y)
        #         params['gamma'] = float(gsearch2.best_params_['gamma'])
        #         print "gamma is ok"
        #         gsearch3 = GridSearchCV(estimator = xgb.XGBRegressor(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight'],
        #                 gamma = gsearch2.best_params_['gamma']),
        #                                 param_grid={'subsample':[i/10.0 for i in range(1,10)],
        #                                             'colsample_bytree':[i/10.0 for i in range(2,10)]})
        #         gsearch3.fit(train_test_X,train_test_Y)
        #         params['subsample'] = gsearch3.best_params_['subsample']
        #         params['colsample_bytree'] = gsearch3.best_params_['colsample_bytree']
        #         print "subsample and colsample_bytree are ok"
        #         gsearch4 = GridSearchCV(estimator = xgb.XGBRegressor(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight'],
        #                 gamma = gsearch2.best_params_['gamma'],
        #                 subsample = gsearch3.best_params_['subsample'],
        #                 colsample_bytree = gsearch3.best_params_['colsample_bytree']),
        #                                 param_grid={'reg_alpha':[1e-5,1e-2,0.1,1,100]})
        #         gsearch4.fit(train_test_X,train_test_Y)
        #         params['reg_alpha'] = float(gsearch4.best_params_['reg_alpha'])
        #         print "reg_alpha is ok"

        #         gsearch5 = GridSearchCV(estimator = xgb.XGBRegressor(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight'],
        #                 gamma = gsearch2.best_params_['gamma'],
        #                 subsample = gsearch3.best_params_['subsample'],
        #                 colsample_bytree = gsearch3.best_params_['colsample_bytree'],
        #                 reg_alpha = gsearch4.best_params_['reg_alpha']),
        #                                 param_grid={'lin_lambda':[1e-5,1e-2,0.1,1,100]})
        #         gsearch5.fit(train_test_X,train_test_Y)
        #         params['lin_lambda'] = float(gsearch4.best_params_['lin_lambda'])
        #         print "lin_lambda is ok"
        #         print params


        train_X, val_X, train_Y, val_Y = train_test_split(train_test_X, train_test_Y,
                                                          test_size=0.2, random_state=3)
        dval = xgb.DMatrix(val_X, label=val_Y)
        dtrain = xgb.DMatrix(train_X, label=train_Y)
        watchlist = [(dtrain, 'train'), (dval, 'val')]
        evals_result = {}
        model_final = xgb.train(params, dtrain, num_boost_round=num_boost_round, early_stopping_rounds=100,
                                # feval=feval_ks_calc,
                                evals=watchlist,
                                verbose_eval=200,
                                evals_result=evals_result
                                )

        s = pd.DataFrame.from_dict(model_final.get_score(importance_type=importance_type), orient='index')
        s.columns = ['weight']
        f_top500 = s.sort_values(by='weight', ascending=False).head(500)
        df = train_test_X
        Xscore = df.loc[:, list(f_top500.index)]
        XX = xgb.DMatrix(train_test_X)
        y_score = model_final.predict(XX)
        return min(evals_result['val']['mae']), model_final, y_score


    elif modeltype == "log":
        params = {
            'booster': 'gbtree',
            'objective': 'binary:logistic',
            'scale_pos_weight': 484130 / 7183,
            'eval_metric': 'auc',
            'gamma': 0.2,
            'silent': 1,
            'max_depth': 4,
            'lambda': 1000,
            'subsample': 0.7,
            'colsample_bytree': 0.3,
            'min_child_weight': 2.5,
            'eta': 0.016,
            'nthread': 12
        }
        data1 = np.mat(X[1:, :], dtype=float)
        head1 = X[0, :]
        head1 = list(np.array(head1)[0])
        datamore = pd.DataFrame(data1, columns=head1)

        #     print datamore.shape
        #     print datamore2.shape
        train_test_X = datamore
        train_test_Y = pd.Series(np.array(label.T)[0])

        #         gsearch1 = GridSearchCV(estimator = xgb.XGBClassifier(params),
        #                                 param_grid={'max_depth':range(1,10),
        #                                             'min_child_weight':[i/2.0 for i in range(1,10)]})
        #     #     print params
        #         gsearch1.fit(train_test_X,train_test_Y)
        #         params['max_depth'] = gsearch1.best_params_['max_depth']
        #         params['min_child_weight'] = (gsearch1.best_params_['min_child_weight'])
        #         print "max_depth and min_child_weight are ok"

        #         gsearch2 = GridSearchCV(estimator = xgb.XGBClassifier(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight']),
        #                                 param_grid={'gamma':[i/10.0 for i in range(10)]})
        #         gsearch2.fit(train_test_X,train_test_Y)
        #         params['gamma'] = float(gsearch2.best_params_['gamma'])
        #         print "gamma is ok"

        #         gsearch3 = GridSearchCV(estimator = xgb.XGBClassifier(
        #                 max_depth=gsearch1.best_params_['max_depth'],
        #                 min_child_weight = gsearch1.best_params_['min_child_weight'],
        #                 gamma = gsearch2.best_params_['gamma']),
        #                                 param_grid={'subsample':[i/10.0 for i in range(1,10)],
        #                                             'colsample_bytree':[i/10.0 for i in range(2,10)]})
        #         gsearch3.fit(train_test_X,train_test_Y)
        #         params['subsample'] = gsearch3.best_params_['subsample']
        #         params['colsample_bytree'] = gsearch3.best_params_['colsample_bytree']
        #         print "subsample and colsample_bytree are ok"


        #     print params


        train_X, val_X, train_Y, val_Y = train_test_split(train_test_X, train_test_Y,
                                                          test_size=0.2, random_state=3)
        dval = xgb.DMatrix(val_X, label=val_Y)
        dtrain = xgb.DMatrix(train_X, label=train_Y)
        watchlist = [(dtrain, 'train'), (dval, 'val')]
        evals_result = {}
        watchlist = [(dtrain, 'train'), (dval, 'val')]
        model_1 = xgb.train(params, dtrain, num_boost_round=num_boost_round, early_stopping_rounds=100,
                            # feval=feval_ks_calc,
                            verbose_eval=50,
                            evals=watchlist, evals_result=evals_result)

        s = pd.DataFrame.from_dict(model_1.get_score(importance_type=importance_type), orient='index')
        s.columns = ['weight']
        f_top500 = s.sort_values(by='weight', ascending=False).head(500)
        df = train_test_X
        Xscore = df.loc[:, list(f_top500.index)]
        XX = xgb.DMatrix(train_test_X)
        y_score = model_1.predict(XX)
        return max(evals_result['val']['auc']), model_1, y_score


# # IV值计算

# In[4]:

def forIV(X_3, label, IVlabelthre):
    '''
    迭代计算IV值

    paramters：
    =======================
    X_3             np.matrix,输入数据,第一列为列名
    label           标签值
    IVlabelthre     IV的阈值，超过该值为1，小于该值为0

    '''
    print "label start", y
    labelk = label
    for i in range(labelk.shape[0]):
        if int(labelk[i]) > IVlabelthre:
            labelk[i] = 1
        else:
            labelk[i] = 0
    data = np.mat(X_3[1:, :], dtype=float)
    head = X_3[0, :]
    head = list(np.array(head)[0])
    head.append('label')
    datamore = np.hstack((data, labelk))
    datamore = pd.DataFrame(datamore, columns=head)
    lst = []
    for colname in datamore.columns:
        if colname != 'label':
            lst.append(calc_feature_woe_iv(datamore, colname))
    f = pd.DataFrame(lst)
    top = f.sort_values(by=0, ascending=False).head(500)
    X_4 = X_3[:, list(top.index)]
    print "label final", y
    return X_4


def calc_feature_woe_iv(data, tgt_col_name, col_type='continuous', bin_size=10, verbose=False):
    """
    根据用户自定义分组来统计good/bad个数，已经对应的WOE，IV值,版本2
    :param data: 进行分组计算的数据集名称
    :param tgt_col_name: 目标变量名
    :param col_type: 变量类型 。默认连续性
    :param bin_size: 分桶大小。 连续性特征默认10个通
    :param verbose: 输出中间过程
    continuous 类型分箱处理
    discrete  按照类型处理
    """
    # check
    if 'label' not in data.columns:
        print 'not label in'
        return ''
    # 计算总体样本量，good/bad总数
    dataset = data.loc[:, ['label', tgt_col_name]].copy()
    dataset.dropna(inplace=True)
    dataset.fillna(-1)
    total_good = len(dataset[dataset.label == 0])
    total_bad = len(dataset[dataset.label == 1])
    total_count = len(dataset)
    rank_col = 'rn_' + tgt_col_name
    if col_type == 'continuous':
        dataset[rank_col] = dataset[tgt_col_name].rank(method='max') / (total_count / bin_size)
        dataset.fillna(-999)
        dataset[rank_col] = dataset[rank_col].apply(lambda x: int(x) if x > 0 else -1)
    if col_type == 'discrete':
        size = dataset[tgt_col_name].unique().size
        if size > 100:
            print 'discrete is too much. size is {}'.format(size)
        dataset[rank_col] = dataset[tgt_col_name]
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
        print tgt_col_name, '  total_iv', total_iv
    return total_iv


# # 多模型求平均，筛选特征

# In[5]:

def pipelinelin(iteration, random_seed, gamma, max_depth,
                lambd, subsample, colsample_bytree, min_child_weight, dtrain, dval):
    params = {
        'booster': 'gbtree',
        'objective': 'reg:linear',
        # 'scale_pos_weight': float(len(y)-sum(y))/float(sum(y)),
        'eval_metric': 'mae',
        'gamma': gamma,
        'silent': 1,
        'max_depth': max_depth,
        'lambda': lambd,
        'subsample': subsample,
        'colsample_bytree': colsample_bytree,
        'min_child_weight': min_child_weight,
        'eta': 0.04,
        'seed': random_seed,
        'nthread': 12
    }

    watchlist = [(dtrain, 'train'), (dval, 'val')]
    model = xgb.train(params, dtrain, num_boost_round=6000,
                      evals=watchlist, verbose_eval=200,
                      early_stopping_rounds=100)

    s = pd.DataFrame.from_dict(model.get_score(importance_type="gain"), orient='index')
    s.columns = ['weight']
    try:
        f = s + f
    except:
        global f
        f = s


def modelchooselin(train_test_X, train_test_Y, xgbnum=10):
    '''
    参数选择
    '''
    train_test_Y = pd.Series(np.array(train_test_Y.T)[0])
    train_test_X1 = pd.DataFrame(np.mat(train_test_X[1:, :], dtype=float))
    #     print train_test_X1.shape
    train_X, val_X, train_Y, val_Y = train_test_split(
        train_test_X1, train_test_Y, test_size=0.2, random_state=3)
    global f
    f = 0
    dval = xgb.DMatrix(val_X, label=val_Y)
    dtrain = xgb.DMatrix(train_X, label=train_Y)
    random_seed = range(1000, 2000, 10)
    gamma = [i / 1000.0 for i in range(100, 200, 1)]
    max_depth = [6, 7, 8]
    lambd = [0.1, 1, 2, 10, 0.01]
    subsample = [i / 1000.0 for i in range(500, 800, 2)]
    colsample_bytree = [i / 1000.0 for i in range(250, 350, 1)]
    min_child_weight = [i / 1000.0 for i in range(200, 300, 1)]
    random.shuffle(random_seed)
    random.shuffle(gamma)
    random.shuffle(max_depth)
    random.shuffle(lambd)
    random.shuffle(subsample)
    random.shuffle(colsample_bytree)
    random.shuffle(min_child_weight)
    # train xgbnum xgb
    for i in range(xgbnum):
        pipelinelin(i, random_seed[i], gamma[i], max_depth[i % 3], lambd[i % 5], subsample[i], colsample_bytree[i],
                    min_child_weight[i], dtrain, dval)
    f_top500 = f.sort_values(by='weight', ascending=False).head(500)
    df = pd.DataFrame(train_test_X)
    df.columns = [int(col) for col in df.columns]
    f_top500.index = [int(col2) for col2 in f_top500.index]
    f_top500_df = df.loc[:, list(f_top500.index)]
    f_top500_ma = np.mat(f_top500_df)
    return f_top500_ma


# In[6]:

def pipelinelog(iteration, random_seed, gamma, max_depth,
                lambd, subsample, colsample_bytree, min_child_weight, dtrain, dval):
    params = {
        'booster': 'gbtree',
        'objective': 'binary:logistic',
        'scale_pos_weight': 484130 / 7183,
        'eval_metric': 'auc',
        'gamma': 0.2,
        'silent': 1,
        'max_depth': 4,
        'lambda': 1000,
        'subsample': 0.7,
        'colsample_bytree': 0.3,
        'min_child_weight': 2.5,
        'eta': 0.016,
        'nthread': 12
    }

    watchlist = [(dtrain, 'train'), (dval, 'val')]
    evals_result = {}
    model_x = xgb.train(params, dtrain, num_boost_round=10000, early_stopping_rounds=100,
                        # feval=feval_ks_calc,
                        verbose_eval=50,
                        evals=watchlist, evals_result=evals_result)

    s = pd.DataFrame.from_dict(model_x.get_score(importance_type="gain"), orient='index')
    s.columns = ['weight']
    try:
        f = s + f
    except:
        global f
        f = s


def modelchooselog(train_test_X, train_test_Y, xgbnum=2):
    '''
    参数选择
    '''
    train_test_Y = pd.Series(np.array(train_test_Y.T)[0])
    train_test_X1 = pd.DataFrame(np.mat(train_test_X[1:, :], dtype=float))
    #     print train_test_X1.shape
    train_X, val_X, train_Y, val_Y = train_test_split(
        train_test_X1, train_test_Y, test_size=0.5, random_state=3)
    global f
    f = 0
    dval = xgb.DMatrix(val_X, label=val_Y)
    dtrain = xgb.DMatrix(train_X, label=train_Y)
    random_seed = range(1000, 2000, 10)
    gamma = [0.1, 0.2, 0, 0.3, 0.4]
    max_depth = [6, 7, 8]
    lambd = [1000, 100, 10, 900, 1100]
    subsample = [i / 1000.0 for i in range(500, 800, 2)]
    colsample_bytree = [i / 1000.0 for i in range(250, 350, 1)]
    min_child_weight = [i / 100.0 for i in range(200, 300, 1)]
    random.shuffle(random_seed)
    random.shuffle(gamma)
    random.shuffle(max_depth)
    random.shuffle(lambd)
    random.shuffle(subsample)
    random.shuffle(colsample_bytree)
    random.shuffle(min_child_weight)
    # train xgbnum xgb
    for i in range(xgbnum):
        pipelinelog(i, random_seed[i], gamma[i], max_depth[i % 3], lambd[i % 5], subsample[i], colsample_bytree[i],
                    min_child_weight[i], dtrain, dval)
    f_top500 = f.sort_values(by='weight', ascending=False).head(1000)
    df = pd.DataFrame(train_test_X)
    df.columns = [int(col) for col in df.columns]
    f_top500.index = [int(col2) for col2 in f_top500.index]
    f_top500_df = df.loc[:, list(f_top500.index)]
    f_top500_ma = np.mat(f_top500_df)
    return f_top500_ma


# # 特征相关性去除

# In[7]:

def dumpResult2(result):  # 数据存储到final里
    try:
        ll = result.shape
        try:
            final = np.hstack((result, final))
        except:
            global final
            final = result
            #     print "final.shape",final.shape
    except:
        ll = result


def removefeature(num, col, x, feature, corr):
    '''
    多进程跑特征间的高相关去除
    '''
    #     print "start",col
    n = 0
    for i in range(x.shape[1]):
        bad = 0
        for j in range(col * num + i + 1, feature.shape[1]):
            t = np.mat(feature[1:, j], dtype=float)
            xz = np.mat(x[1:, i], dtype=float)
            if corrfunction(t, xz) >= corr:
                bad = 1
                break;
        if bad == 0:
            try:
                n = np.hstack((n, x[:, i]))
            except:
                n = x[:, i]

    try:
        ll = n.shape
        return n

    except:
        return 0


# print "n.shape",n.shape



def f2fremove(trr, block=16, process_num=8, f2fcorr=0.9):
    '''
    #         parameters:
#         --------------
#   X:                    np.matrix，传入的feature
#   f2fcorr:             特征之间高于此值便删除其中一个
#   block:               将原数据分块的数目，影响速度,    default = 16
#   process_num:          进程数，控制进程数量，越大使用的进程数越多，default=8
#
    '''
    global final
    final = 0
    pool = Pool(processes=process_num)
    num = trr.shape[1] / block
    for t in range(block):  # 对矩阵块进行分割来跑去除
        if t == block - 1:
            result = pool.apply_async(removefeature, (num, t, trr[:, (num) * t:], trr, f2fcorr), callback=dumpResult2)
        else:
            result = pool.apply_async(removefeature, (num, t, trr[:, (num) * t:(num * (t + 1))], trr, f2fcorr),
                                      callback=dumpResult2)
    pool.close()
    pool.join()
    #     if result.successful():
    #         print "successful"
    #     print "final.shape:",final.shape
    return final


# # 正在写的相关性

# In[8]:

def dumpResult3(result):  # 数据存储到final里
    try:
        ll = result.shape
        try:
            finalkk = np.hstack((result, finalkk))
        except:
            global finalkk
            finalkk = result
            #     print "finalkk.shape",finalkk.shape
    except:
        ll = result


def f2be(num, col, x, feature, corr):
    '''
    多进程跑特征间的高相关去除
    '''
    n = 0
    #     print "第{}次".format(col)
    #     print x.shape
    #     print feature.shape
    for i in range(x.shape[1]):
        bad = 0
        for j in range(feature.shape[1]):
            t = np.mat(feature[1:, j], dtype=float)
            xz = np.mat(x[1:, i], dtype=float)
            if corrfunction(t, xz) >= corr:
                bad = 1
                break;
        if bad == 0:
            #             print "ok"
            try:
                n = np.hstack((n, x[:, i]))
            except:
                n = x[:, i]
    try:
        ll = n.shape
        return n
    except:
        return 0


def f2before(trr, P, block=20, process_num=8, f2beforecorr=0.8):
    '''
    #         parameters:
#         --------------
#   X:                    np.matrix，传入的feature
#   f2fcorr:             特征之间高于此值便删除其中一个
#   block:               将原数据分块的数目，影响速度,    default = 16
#   process_num:          进程数，控制进程数量，越大使用的进程数越多，default=8
#
    '''
    global finalkk
    finalkk = 0
    pool = Pool(processes=process_num)
    num = trr.shape[1] / block
    for t in range(block):  # 对矩阵块进行分割来跑去除
        if t == block - 1:
            result = pool.apply_async(f2be, (num, t, trr[:, (num) * t:], P, f2beforecorr,), callback=dumpResult3)
        else:
            result = pool.apply_async(f2be, (num, t, trr[:, (num) * t:(num * (t + 1))], P, f2beforecorr,),
                                      callback=dumpResult3)
    pool.close()
    pool.join()
    #     if result.successful():
    #         print "successful"
    return finalkk


# # 相关性函数

# In[9]:

def corrfunction(X1, X2, method='pearson'):
    '''
#     pamameters
#     -------------
#     X1,X2 :   np.matrix
#     method:   相关性的判断方法，pearson,minepy,spearman,kendall

    '''
    X1 = pd.Series(np.array(X1.reshape(-1, 1)).T[0])
    X2 = pd.Series(np.array(X2.reshape(-1, 1)).T[0])
    for case in switch(method):  # 进行选择相关性
        if case('MIC'):
            mine = minepy.MINE(alpha=0.6, c=15, est="mic_approx")
            mine.compute_score(X1, X2)
            corr = mine.mic()
            break
        if case("pearson"):
            corr = X1.corr(X2)
            break
        if case("spearman"):
            corr = X1.corr(X2, method="spearman")
            break
        if case("kendall"):
            corr = X1.corr(X2, method="kendall")
            break
    return abs(corr)


# # 特征交叉

def cross_cal(op, X1, X2, label, retfeat, i, v, stdnum, nannum, feature2label, corrtype):
    for case in switch(op):
        if case("*"):
            a = np.multiply(np.mat(X1[1:, i], dtype=float), np.mat(X2[1:, v], dtype=float))  # x*y
            head = np.mat(["{} * {}".format(str(X1[0, i]), str(X2[0, v]))])  # 保存header
            break
        if case("+"):
            a = np.mat(X1[1:, i], dtype=float) + np.mat(X2[1:, v], dtype=float)  # x+y
            head = np.mat(["{} + {}".format(str(X1[0, i]), str(X2[0, v]))])  # 保存header
            break
        if case("-"):
            a = np.mat(X1[1:, i], dtype=float) - np.mat(X2[1:, v], dtype=float)  # x-y
            head = np.mat(["{} - {}".format(str(X1[0, i]), str(X2[0, v]))])  # 保存header
            break
        if case("/"):
            with np.errstate(divide='ignore', invalid='ignore'):
                a = np.true_divide(np.mat(X1[1:, i], dtype=float), np.mat(X2[1:, v], dtype=float))
                a[a == np.inf] = -999
                # x/y
                head = np.mat(["{}/{}".format(str(X1[0, i]), str(X2[0, v]))])  # 保存header
            break
        if case("1/x"):
            oney = mat(np.ones(X1.shape[0] - 1)).T
            with np.errstate(divide='ignore', invalid='ignore'):  # 1/x
                a = np.true_divide(oney, np.mat(X1[1:, i], dtype=float))
                a[a == np.inf] = -999
            head = np.mat(["1/{}".format(str(X1[0, i]))])  # 保存header
            break
        if case("1/x+1/y"):  # 1/x+1/y
            oney = mat(np.ones(X1.shape[0] - 1)).T
            with np.errstate(divide='ignore', invalid='ignore'):
                a = np.true_divide(oney, np.mat(X1[1:, i], dtype=float)) + np.true_divide(oney, np.mat(X2[1:, v],
                                                                                                       dtype=float))
                a[a == np.inf] = -999
            head = np.mat(["1/{} + 1/{}".format(str(X1[0, i]), str(X2[0, v]))])  # 保存header
            break
        if case("x^2+y^2"):  # x^2+y^2
            oney = mat(np.ones(X1.shape[0] - 1)).T
            kt1 = np.mat(X1[1:, i], dtype=float)
            kt2 = np.mat(X2[1:, v], dtype=float)
            a = np.multiply(kt1, kt1) + np.multiply(kt2, kt2)  # 保存header
            head = np.mat(["{}^2 + {}^2".format(str(X1[0, i]), str(X2[0, v]))])
            break
        if case("log"):  # x^2+y^2
            kt1 = np.mat(X1[1:, i], dtype=float)
            a = np.log(kt1)
            a[a == -np.inf] = 0
            head = np.mat([" log({})".format(str(X1[0, i]))])
            break
        if case("fft"):
            ff = np.mat(X1[1:, i], dtype=float)
            a = np.nan_to_num(ff)
            a = np.fft.fft(a)
            head = np.mat([" fft({})".format(str(X1[0, i]))])
    corralabel = corrfunction(a, label, method=corrtype)
    if (corrfunction(a, np.mat(X1[1:, i], dtype=float)) < 0.8) and (
    corrfunction(a, np.mat(X2[1:, v], dtype=float) < 0.8)) and (
        corralabel > (corrfunction(np.mat(X1[1:, i], dtype=float), label))) and (
        corralabel > (corrfunction(np.mat(X2[1:, v], dtype=float), label))):
        if (corralabel >= feature2label):  # 相关性判断
            #             if corrfunction(a,label,method=corrtype)>0.3:
            #                 print head
            if (np.nanstd(a) > stdnum):  # 方差判断
                u = pd.Series(np.array(a.reshape(1, -1))[0])  # 为了进行判空而进行的变换
                if (sum(u.isnull()) < nannum):  # 空值率判断
                    a = np.vstack((head, a))
                    retfeat = np.hstack((retfeat, a))
                    #     print retfeat.shape
    return retfeat  # 保留的结合起来


def dumpResult1(result):
    #     print "pickle start"
    col = result[0, 0]
    result = result[:, 1:]
    fileName = pathpickle + "/" + str(col) + ".pkl"
    global ss
    sts = ss
    global filenamek
    if sts == "/":
        sts = "k"
    elif sts == "1/x":
        sts = "l"
    elif sts == "1/x+1/y":
        sts = "m"
    while (True):
        if os.path.exists(fileName):
            fileName = pathpickle + "/" + str(col) + str(sts) + ".pkl"
            sts = sts + "1"
        else:
            break
    f = file(fileName, "wb")
    filenamek.append(fileName)
    if result.shape[1] > 1:
        result = f2fremove(result, block=2, process_num=16, f2fcorr=0.4)
    pickle.dump(result, f)
    f.close()


#     print result.shape
#     print "pickle success"



def cross(P, t, X1, X2, label, col_type, op, feature2label, corrtype, stdnum, nullnum):  # 特征交叉
    #     print "start",t
    #     print X1.shape
    nannum = nullnum * (X1.shape[0] - 1)
    retfeat = np.ones((X1.shape[0], 1)) * t
    try:
        if ((P[0, :] == X2[0, :]).all()):  # 如果无领域交叉，便需要判断数据是否重复处理
            for i in range(X1.shape[1]):
                if (op == "1/x") or (op == "log") or (op == "fft"):
                    retfeat = cross_cal(op, X1, X1, label, retfeat, i, i, stdnum, nannum, feature2label, corrtype)
                else:
                    for v in range(X2.shape[1]):
                        if (X1[0, i] != X2[0, v]):
                            if (i in col_type) or (v in col_type):
                                retfeat = cross_cal("*", X1, X2, label, retfeat, i, v, stdnum, nannum, feature2label,
                                                    corrtype="spearman")
                            elif (i not in col_type) and (v not in col_type):
                                retfeat = cross_cal(op, X1, X2, label, retfeat, i, v, stdnum, nannum, feature2label,
                                                    corrtype)  # 保留的结合起来

        else:  # 如果有领域分类，便无需判断重复
            for i in range(X1.shape[1]):
                if (op == "1/x") or (op == "log") or (op == "fft"):
                    retfeat = cross_cal(op, X1, X1, label, retfeat, i, i, stdnum, nannum, feature2label, corrtype)
                else:
                    for v in range(X2.shape[1]):
                        if (i in col_type) or (v in col_type):
                            retfeat = cross_cal("*", X1, X2, label, retfeat, i, v, stdnum, nannum, feature2label,
                                                corrtype="spearman")
                        elif (i not in col_type) and (v not in col_type):
                            retfeat = cross_cal(op, X1, X2, label, retfeat, i, v, stdnum, nannum, feature2label,
                                                corrtype)
                            # 保留的结合起来
    except:
        for i in range(X1.shape[1]):
            if (op == "1/x") or (op == "log") or (op == "fft"):
                retfeat = cross_cal(op, X1, X1, label, retfeat, i, i, stdnum, nannum, feature2label, corrtype)
            else:
                for v in range(X2.shape[1]):
                    if (i in col_type) or (v in col_type):
                        retfeat = cross_cal("*", X1, X2, label, retfeat, i, v, stdnum, nannum, feature2label,
                                            corrtype="spearman")
                    elif (i not in col_type) and (v not in col_type):
                        retfeat = cross_cal(op, X1, X2, label, retfeat, i, v, stdnum, nannum, feature2label, corrtype)
                        # 保留的结合起来
                        #     print "file shape:",retfeat.shape
                        #     print "finish",t
    return retfeat


def feature_cross(X1, X2, label, op="*", corrtype='pearson', feature2label=0.1, nullnum=0.9, stdnum=0, block=2,
                  process_num=4, col_type=[]):
    global ss
    ss = op
    print op
    global filenamek
    filenamek = []
    old_err_state = np.seterr(divide='raise')  # 保证一个数
    ignored_states = np.seterr(**old_err_state)  # 除以0为0
    pool = Pool(processes=process_num)
    num = X1.shape[1] / block
    for col in range(block):
        if col == (block - 1):
            result = pool.apply_async(cross, (
            X1, col, X1[:, num * col:], X2, label, col_type, op, feature2label, corrtype, stdnum, nullnum),
                                      callback=dumpResult1)
        else:
            result = pool.apply_async(cross, (
            X1, col, X1[:, num * col:num * (col + 1)], X2, label, col_type, op, feature2label, corrtype, stdnum,
            nullnum), callback=dumpResult1)
    pool.close()
    pool.join()
    if result.successful():
        print "result successful"
    return filenamek


# # 利用GBDT造特征

# In[11]:



def gbdtmake(X, y, modeltype, depth=4, treenum=500, process_num=10, model_num=10, block=40):
    '''
     parameter
     ==============================
    #X,y              np.mat, X带有header,比y多一列
    #modeltype        线性模型或者回归模型
    #depth            树的深度，最好不超过5。defult=4
    #treenum          树的个数。defult=300
    #process_num      进程数。 default=10
    #model_num        模型个数。default=10
    #block            数据分割块数。default=40

    #return:          原数据与新数据的结合。np.matrix
    '''
    head = X[0, :]
    X = np.mat(X[1:, :], dtype=float)
    if modeltype == "lin":
        params = {
            'booster': 'gbtree',
            'objective': 'reg:linear',
            # 'scale_pos_weight': float(len(y)-sum(y))/float(sum(y)),
            'eval_metric': 'mae',
            'gamma': 0.2,
            'silent': False,
            'max_depth': 7,
            'lambda': 10,
            'reg_alpha': 0,
            'colsample_bytree': 0.3,
            'min_child_weight': 2.5,
            'eta': 0.015,
            'nthread': 18
        }
    elif modeltype == "log":
        params = {
            'booster': 'gbtree',
            'objective': 'binary:logistic',
            'scale_pos_weight': 484130 / 7183,
            'eval_metric': 'auc',
            'gamma': 0.2,
            'silent': 1,
            'max_depth': 4,
            'lambda': 1000,
            'subsample': 0.7,
            'colsample_bytree': 0.3,
            'min_child_weight': 2.5,
            'eta': 0.016,
            'nthread': 18
        }

    train_X, val_X, train_Y, val_Y = train_test_split(X, y,
                                                      test_size=0.01, random_state=3)
    dval = xgb.DMatrix(val_X, label=val_Y)
    dtrain = xgb.DMatrix(train_X, label=train_Y)
    watchlist = [(dtrain, 'train'), (dval, 'val')]
    evals_result = {}
    model_aft = xgb.train(params, dtrain, num_boost_round=treenum, early_stopping_rounds=100,
                          # feval=feval_ks_calc,
                          evals=watchlist,
                          verbose_eval=200,
                          evals_result=evals_result
                          )
    # 得到树模型
    trees = model_aft.get_dump()

    # 将树模型转换成可以看的模型
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
                    z["leaf"] = int(arr[0].split(":")[0])
                    x = x + 1
                    z["leafrank"] = x
                    big[int(arr[0].split(":")[0])] = z
                except:
                    continue
            else:
                k["num"] = int(arr[0].split(":")[0])
                k["featurename"] = int(arr[1].split()[0].split("<")[0].split("f")[1])
                k["featurenametrue"] = head[0, int(arr[1].split()[0].split("<")[0].split("f")[1])]
                k["splitpoint"] = float(arr[1].split()[0].split("<")[1].split("]")[0])
                k["yesnext"] = int(arr[1].split("]")[1].split("=")[1].split(",")[0])
                k["nonext"] = int(arr[1].split("]")[1].split("=")[1].split(",")[0]) + 1
                k["missing"] = int(arr[1].split("]")[1].split("=")[3])
                k["leaf"] = "NONE"
                k["yes_splitname"] = str(
                    "(" + str(k["featurenametrue"]) + " < " + str(k["featurenametrue"]) + ")" + " --> ")
                k["no_splitname"] = str(
                    "(" + str(k["featurenametrue"]) + " >= " + str(k["featurenametrue"]) + ")" + " --> ")
                big[int(arr[0].split(":")[0])] = k
        big["leafnum"] = x
        if x != 1:
            Lst.append(big)

    # 结合数据
    newfeatall = mergedata(X, y, Lst, process_num, block)
    gbdt_num = newfeatall.shape[1] - X.shape[1]
    lst = []
    for i in range(gbdt_num - 1):
        l = "GBDT{}".format(i)
        lst.append(l)
    lst.append("label")
    lst = np.mat(lst)
    head = np.hstack((head, lst))
    newfeatall = np.vstack((head, newfeatall))
    return newfeatall, gbdtnameall2


def mergedata(X, y, Lst, process_num, block=20):
    global newfeatall
    newfeatall = 0
    pool = Pool(processes=process_num)
    num = X.shape[0] / block
    index = np.mat(range(X.shape[0])).T
    X = np.hstack((index, X))
    for t in range(block):
        if t == block - 1:
            result = pool.apply_async(mergesplit, (Lst, X[(num) * t:, :], y[(num) * t:, :],), callback=mergeresult)
        else:
            result = pool.apply_async(mergesplit,
                                      (Lst, X[(num) * t:(num * (t + 1)), :], y[(num) * t:(num * (t + 1)), :],),
                                      callback=mergeresult)
    pool.close()
    pool.join()
    newfeatall_sort = pd.DataFrame(newfeatall).sort_values(by=0)
    newfeatall = np.mat(newfeatall_sort)
    return newfeatall[:, 1:]


def mergesplit(Lst, x, y):
    for wq in range(x.shape[0]):
        X = x[wq, 1:]
        zeros = 0
        count = 0
        for tere in Lst:
            i = 0
            while (tere[i]["leaf"] == "NONE"):
                try:
                    int(X[0, tere[i]["featurename"]])
                    if X[0, tere[i]["featurename"]] < tere[i]["splitpoint"]:
                        i = tere[i]["yesnext"]
                    else:
                        i = tere[i]["nonext"]
                except:
                    i = tere[i]["missing"]
            zero = np.zeros(tere["leafnum"])
            zero[tere[i]["leafrank"] - 1] = 1
            if count == 0:
                zeros = zero
            else:
                zeros = np.hstack((zeros, zero))
            count = count + 1
        # merge_X_feat = np.hstack((X,np.mat(zeros)))
        #         merge_Xy_feat = np.hstack((merge_X_feat,y[wq,:]))
        if wq == 0:
            newfeat = zeros
        else:
            newfeat = np.vstack((newfeat, zeros))
    newfeat = np.hstack((x, np.mat(newfeat)))
    newfeat = np.hstack((newfeat, y))
    return newfeat


def mergeresult(result):
    try:
        newfeatall = np.vstack((result, newfeatall))
    except:
        global newfeatall
        newfeatall = result


# print "merge",newfeatall.shape


# # 模型可视化

# In[12]:

class see:
    def __init__(self, model, topk, y_score, label, gbdtnameall):
        self.model = model
        self.topk = topk
        self.y_score = y_score
        self.label = label
        self.gbdtnameall = gbdtnameall

    def ks(self):
        fpr, tpr, th = roc_curve(self.label, self.y_score)
        b = tpr - fpr
        b1 = np.where(b == np.max(b))
        ks = max(b)
        y = []
        y.append((tpr[b1]))
        y.append((fpr[b1]))
        x = [int(b1[0]), int(b1[0])]
        plt.plot(fpr, label="FPR")
        plt.plot(tpr, label="TPR")
        plt.plot(x, y)
        plt.title("KS graph\nKS = %0.2f" % ks)
        plt.legend(loc="mid right")
        plt.show()

    def rocplot(self):
        fpr, tpr, th = roc_curve(self.label, self.y_score)
        roc_auc = auc(fpr, tpr)
        plt.plot(fpr, tpr, lw=1, label='ROC fold (auc = %0.2f)' % (roc_auc))
        plt.xlim([-0.05, 1.05])
        plt.ylim([-0.05, 1.05])
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title('ROC')
        plt.legend(loc="lower right")
        plt.show()

    def feature_importance(self):
        s = pd.DataFrame.from_dict(self.model.get_score(importance_type="gain"), orient='index')
        s.columns = ['weight']
        f_top = s.sort_values(by='weight', ascending=False).head(self.topk)  # 可以修改展示特征的个数，初始化为20
        f_top.plot(kind='barh')
        plt.show()



