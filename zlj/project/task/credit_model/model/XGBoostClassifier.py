#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import xgboost as xgb
import numpy as np
from ml_metrics import rmse
from ml_metrics import auc
class XGBoostClassifier():
    def __init__(self, **params):
        self.clf = None
        # self.num_boost_round = params['num_round']
        self.params = params
        self.params.update({'objective': 'binary:logistic'})
        self.params.update({'silent':1})

    def fit(self, X, y,val_X,val_y):
        num_boost_round = self.params['num_round']
        # num_boost_round = 1900
        dtrain = xgb.DMatrix(X, label=y,missing=-1)
        dval = xgb.DMatrix(val_X, label=val_y,missing=-1)
        watchlist  = [(dtrain,'train'),(dval,'val')]
        # params = self.params
        if X.shape[1]==1:
            self.params.update({'colsample_bytree': 1.0})
        self.clf = xgb.train(
                self.params,
                dtrain,
                num_boost_round=num_boost_round ,
                early_stopping_rounds=100,
                evals=watchlist
        )
        self.fscore = self.clf.get_fscore()

        # bb=np.zeros(dtrain.num_col())
        bb = {}

        for ftemp, vtemp in self.fscore.items():
            # bb[int(ftemp[1:])]=vtemp
            bb[ftemp] = vtemp

        # bb=bb/float(bb.max())
        i = 0
        cc = np.zeros(dtrain.num_col())
        for feature, value in  bb.items():
            cc[i] = value
            i+=1
        self.coef_= cc

    def predict(self, val_X):
        # dX = xgb.DMatrix(X)
        m,n = val_X.shape
        x_labels = np.array([2]*m)
        dX = xgb.DMatrix(val_X, missing=-1)
        y = self.clf.predict(dX,self.clf.best_ntree_limit)
        return y
    def score(self, X, y):
        Y = self.predict(X)
        return self.rmse_loss(y, Y)

    def scoreAUC(self, val_X, val_y):
        Y = self.predict(val_X)
        return self.auc_score(val_y.values,Y)


    def get_params(self, deep=True):
        return self.params

    def set_params(self, **params):
        # if 'num_boost_round' in params:
        #     self.num_boost_round = params.pop('num_boost_round')
        # if 'objective' in params:
        #     del params['objective']
        self.params.update(params)
        return self

    def rmse_loss(self,y,y_pred):
        return rmse(y,y_pred)

    def auc_score(self, y, y_pred):
        # sorted_y_pred = sorted(y_pred,reverse=True)
        return auc(y,y_pred)