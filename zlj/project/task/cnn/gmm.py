#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from sklearn.mixture import GMM
from matplotlib.pyplot import *
import numpy as np

try:
    import cPickle as pickle
except:
    import pickle

# with open('/path/to/kde.pickle') as f:  # open the data file provided above
#     kde = pickle.load(f)
# kde=[0, 0, 0, 0, 0, 4, 7, 14, 20, 20, 12, 3, 6, 12, 18, 18, 12, 6, 9, 14, 15, 16, 18, 18, 8, 8, 15, 22, 9, 2, 0, 0, 0, 0, 0, 0, 0, 0, 3, 4, 4, 6, 8, 11, 12, 19, 20, 18, 18, 19, 23, 20, 17, 13, 10, 13, 15, 15, 12, 6, 7, 7, 8, 3, 0, 2, 2, 5, 14, 19, 18, 15, 7, 9, 8, 13, 17, 14, 12, 8, 12, 13, 12, 11, 6, 7, 7, 5, 6, 5, 3, 2, 2, 0, 0, 2, 5, 6, 8, 10, 8, 6, 7, 7, 9, 15, 16, 16, 12, 8, 7, 8, 18, 19, 16, 8, 6, 5, 4, 0, 0, 0, 2, 2, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 19, 22, 14, 2, 3, 4, 3, 4, 3, 2, 2, 2, 0, 0, 0]
kde=[0,0,0,0,3,3,2,2,3,4,3,5,10,17,20,18,11,6,6,7,6,5,6,7,5,3,0,0,0,0,0,0,2,2,2,2,3,3,7,8,8,7,10,12,20,24,16,15,10,23,24,24,5,8,20,20,5,5,6,6,11,21,21,7,8,7,5,5,5,8,16,19,19,12,8,11,12,12,12,13,11,10,13,16,20,20,21,12,11,6,4,6,3,4,4,2,5,5,4,9,10,9,7,11,15,14,16,16,15,13,9,6,9,19,20,19,14,4,4,4,0,0,2,2,0,0,0,0,0,0,3,3,3,3,3,3,5,6,6,3,6,20,22,15,6,2,2,2,2,4,4,7,5,5,2,0,0,0,0,0]
# obs = np.concatenate((np.random.randn(100, 1),  10 + np.random.randn(300, 1)))
ss=[]
for i,v in enumerate(kde):
    ss.append([i*1.0,v*1.0])
print ss
gmm = GMM(n_components=5,covariance_type='spherical')
gmm.fit(ss)

x = np.linspace(np.min(ss), np.max(ss), len(ss))

print gmm.n_components,gmm.weights_,gmm.covars_

for i in ss:
    print gmm.predict(i)
# print x
# # Plot the data to which the GMM is being fitted
# figure()
# plot(x, kde, color='blue')