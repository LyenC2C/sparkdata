#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import pandas as pd
import numpy as np

def evel_ks_calc(y_pred,y_true):
	import math
	data=pd.DataFrame({'label':y_true, 'rs':y_pred})
	data=data.sort_index(by=['rs'])
	data['sortindex'] =[i for i in xrange(len(data))]
	data['bucket']= pd.cut(data.sortindex ,10,precision=1)
	group=data.groupby('bucket',as_index=False)
	data['bad']=data['label'].map(lambda x: 1 if x==0 else 0)
	data['good']=data['label'].map(lambda x: 1 if x==1 else 0)
	cumsum=group.sum().cumsum()
	kv=group.sum()

	cumsum['bad_cumsum_p']=cumsum['bad']*1.0/kv.sum()['bad']
	cumsum['good_cumsum_p']=cumsum['good']*1.0/kv.sum()['good']
	cumsum['ks']= cumsum['bad_cumsum_p']-cumsum['good_cumsum_p']
	# ks=np.array([ i for  i in cumsum['ks']]).max()
	return cumsum


y_pred=[0.528307539,
0.253919121,
0.339990015,
0.749575637,
0.926909636,
0.215776335,
0.594408387,
0.005691463,
0.089565652,
0.2443335,
0.112730904,
0.867299051,
0.535796306,
0.200698952,
0.792111832,
0.450324513,
0.761258113,
0.4222666,
0.873389915,
0.296854718,
0.767049426,
0.968647029,
0.832051922,
0.780229656,
0.6222666,
0.373040439,
0.19990015,
0.535796306,
0.236045931,
0.770144783,
0.899251123,
0.233449825,
0.788617074,
0.428457314,
0.818971543,
0.45341987,
0.661208188,
0.050923615,
0.60429356,
0.445831253,
0.770943585,
0.728007988,
0.680279581,
0.720319521,
0.507438842,
0.553569646,
0.159261108,
0.167748377,
0.829755367,
0.332601098,
0.279680479,
0.279680479,
0.384623065,
0.086070894,
0.306440339,
0.891562656,
0.777134299,
0.116225662,
0.98202696,
0.3998003,
0.863804294,
0.175137294,
0.427958063,
0.113130305,
0.781028457,
0.124213679,
0.760259611,
0.925411882,
0.807488767,
0.347978033,
0.347978033,
0.237244134,
0.071992012,
0.071992012,
0.163354968,
0.163354968,
0.296854718,
0.089166251,
0.110034948,
0.190813779,
0.242136795,
0.185222167,
0.504343485,
0.115227159,
0.507938093,
0.52441338,
0.38212681,
0.870294558,
0.23564653,
0.335297054,
0.144782826,
0.205391912,
0.205391912,
0.841038442,
0.323315027,
0.898352471,
0.673589616,
0.718622067,
0.180229656,
0.867498752,
0.377533699,
0.2443335,
0.845132302,
0.776135796,
0.405591613,
0.19111333,
0.225761358,
0.125711433,
0.169345981,
0.145182227,
0.163354968,
0.127309036,
0.177134299,
0.32431353,
0.645232152,
0.050923615,
0.197703445,
0.528407389,
0.241238143,
0.089565652,
0.021767349,
0.459311033,
0.347678482,
0.125511732,
0.625561658,
0.176734898,
0.125511732,
0.537194209,
0.525811283,
0.215776335,
0.229256116,
0.211083375,
0.519620569,
0.027658512,
0.135297054,
0.187019471,
0.174538193,
0.8001997,
0.455017474,
0.516824763,
0.281278083,
0.027259111,
0.136295557,
0.126909636,
0.383524713,
0.067898153,
0.056215676,
0.059011483,
0.068996505,
0.01767349,
0.057114329,
0.200499251,
0.12910634,
0.021867199,
0.255317024,
0.203494758,
0.24463305,
0.037743385,
0.869795307,
0.347978033,
0.119321018,
0.513729406,
0.335197204,
0.32021967,
0.278582127,
0.002496256,
0.365851223,
0.161357963,
0.570444333,
0.336095856,
0.900948577,
0.242136795,
0.185621568]

y_true=[0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
0,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1,
1]

# ks= evel_ks_calc(y_pred,y_true)
# ks.to_csv('E:\\mashagn.test.csv')

import csv
from getAUC import *
from ks_roc import *
#import matplotlib.pyplot as pl
import pickle

import numpy as np
from sklearn import metrics


ks, ks_pos, pctl, tpr, fpr, tp_cumcnt, fp_cumcnt, threshold, lorenz_curve, lorenz_curve_capt_rate= ks_roc(y_true,y_pred)
auc = getAUC(y_pred,y_true)


#for comparison with sklearn
fprx, tprx, thresholds = metrics.roc_curve(y_true,y_pred)
auc=metrics.auc(fprx, tprx)
ks=max(tprx-fprx)


# Compute KS, ROC curve and AUC for validation
# ks_v, ks_pos_v, pctl_v, tpr_v, fpr_v, tp_cumcnt_v, fp_cumcnt_v, threshold_v, lorenz_curve_v, lorenz_curve_capt_rate_v = ks_roc(yv,pv_pred)
# auc_v = getAUC(pv_pred,yv)
#
#
# #for comparison with sklearn
# fpry, tpry, thresholds = metrics.roc_curve(yv, pv_pred)
# auc_v=metrics.auc(fpry, tpry)
# ks_v=max(tpry-fpry)
import matplotlib.pyplot as pl
# pl.figure(1, figsize=(8,8))
# pl.clf()
# pl.plot(fpr, tpr, 'b-', label='ROC Train(AUC=%0.3f)' % auc)
# # pl.plot(fpr_v, tpr_v, 'r--', label='ROC Validation (AUC=%0.3f)' % auc_v)
# pl.plot([0, 1], [0, 1], 'k--')
# pl.xlim([0.0, 1.0])
# pl.ylim([0.0, 1.0])
# pl.xlabel('False Positive Rate')
# pl.ylabel('True Positive Rate')
# pl.title('ROC Curve:  Model')
# pl.legend(loc="lower right")


pl.figure(2, figsize=(8,8))
pl.clf()
pl.plot(pctl, tpr, 'b-',label='Fraud Train',linewidth=2.0 )
pl.plot(pctl, fpr, 'g-',label='Non-Fraud Train',linewidth=2.0)
# pl.plot(pctl_v, tpr_v, 'm--',label='Fraud Validation' )
# pl.plot(pctl_v, fpr_v, 'r--',label='Non-Fraud Validation')
pl.plot([0, 1], [0, 1], 'k--')
pl.xlim([0.0, 1.0])
pl.ylim([0.0, 1.0])
pl.xlabel('Score Percentile')
pl.ylabel('Cumulative Rate')
# tilte_lorenz="Lorenz Curve:KS Train={:.3f}({:.3f});KS Vali={:.3f}({:.3f})".format(float(ks),float(ks_pos),float(ks_v),float(ks_pos_v))
tilte_lorenz="Lorenz Curve:KS Train={:.3f}({:.3f}) ".format(float(ks),float(ks_pos))
pl.title(tilte_lorenz)
pl.legend(loc="lower right")
pl.show()