#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')
import  pandas as pd

from address_valid_v1 import *


file=pd.read_csv(u'E:\\项目\\xiage\\招行客户地址信息.csv')

print file.columns
ad1=file.iloc[:,1]
ad2=file.iloc[:,2]
dis=[]
for k ,v in zip(ad1,ad2):
    try:
        distance=adress_distance(k,v)
        print k,v ,distance
        dis.append(distance)
    except:dis.append(-1)

file['distance']=dis
file.to_csv(u'E:\\项目\\xiage\\招行客户地址信息_distance.csv')

# print ad1