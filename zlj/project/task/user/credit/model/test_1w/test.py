#coding:utf-8
from sklearn.metrics import roc_curve
from sklearn.utils import column_or_1d

__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import matplotlib.pyplot as plt
import pandas  as pd
data=pd.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360\\fix\\step2_ks.csv')
roc = roc_curve(data['label'], data['rs'])
plt.plot(roc[0], roc[1],color='r',label=u'gbdt')
base_line=[0.5   for i in xrange(len(data))]
base_line_roc_test = roc_curve(data['label'], base_line)
plt.plot(base_line_roc_test[0], base_line_roc_test[1],color='y',label=u'base_line')

plt.title('roc')
plt.legend()
plt.show()