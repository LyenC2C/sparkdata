#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')
import  pandas as pd

from address_valid_v1 import *


'''
测试 益芯
'''

path=u'E:\\项目\\3-项目\\5-地址匹配\\'


tb_ad=pd.ExcelFile(path+'淘宝-益芯地址反馈12.19.xlsx').parse('Sheet1').iloc[:,:5].drop_duplicates()
origin_ad=pd.ExcelFile(path+'origin_本益芯地址验证数据.xlsx').parse('地址验证').drop_duplicates()
# print tb_ad.sheet_names

merge_data=origin_ad.merge(tb_ad,on=['MOBILE_NO'])


def fun(x):
    if x==True:return '匹配'
    if x==False:return '不匹配'
# tb_groupby= tb_ad.iloc[:,:5].drop_duplicates().groupby(by=['MOBILE_NO'])
#
# dic={k:v  for k,v in tb_groupby  }

data=merge_data.copy()

def work_match(x):
    MOBILE_NO,name,CORP_PROVINCE,CORP_CITY,CORP_AREA,CORP_DETAIL,FAMILY_PROVINCE,FAMILY_CITY,FAMILY_AREA,FAMILY_DETAIL,tb_name,tb_add_detail,tb_prov,tb_city=x
    corp_adress=CORP_PROVINCE+CORP_CITY+CORP_AREA+CORP_DETAIL
    family_adress=FAMILY_PROVINCE+FAMILY_CITY+FAMILY_AREA+FAMILY_DETAIL

    match_prov,match_city,district,match_address_detail=address_format(corp_adress)[1]
    taobao_gps=address_gps(tb_prov+tb_city+tb_add_detail)
    match_gps=address_gps(corp_adress)
    distance=haversine(match_gps[0],match_gps[1],taobao_gps[0],taobao_gps[1])
    confidence=(100-distance*2)
    confidence= confidence if confidence>0 else 0
    # print match_prov ,match_city,match_address_detail ,':' ,tb_prov,tb_city,tb_add_detail
    return fun(tb_name in name), fun(tb_prov in match_prov), fun(match_city in tb_city), round(distance,3) ,  confidence
def family_match(x):
    MOBILE_NO,name,CORP_PROVINCE,CORP_CITY,CORP_AREA,CORP_DETAIL,FAMILY_PROVINCE,FAMILY_CITY,FAMILY_AREA,FAMILY_DETAIL,tb_name,tb_add_detail,tb_prov,tb_city=x
    corp_adress=CORP_PROVINCE+CORP_CITY+CORP_AREA+CORP_DETAIL
    family_adress=FAMILY_PROVINCE+FAMILY_CITY+FAMILY_AREA+FAMILY_DETAIL

    match_prov,match_city,district,match_address_detail=address_format(family_adress)[1]
    taobao_gps=address_gps(tb_prov+tb_city+tb_add_detail)
    match_gps=address_gps(corp_adress)
    distance=haversine(match_gps[0],match_gps[1],taobao_gps[0],taobao_gps[1])
    confidence=(100-distance*2)
    confidence= confidence if confidence>0 else 0
    # print match_prov ,match_city,match_address_detail ,':' ,tb_prov,tb_city,tb_add_detail
    return fun(tb_name in name), fun(tb_prov in match_prov), fun(match_city in tb_city), round(distance,3) ,  confidence


# test=data.head(100).copy()
# test1=data.head(100).copy()
test=data .copy()
test1=data .copy()
test[u'姓名匹配'],test[u'工作省份匹配'] ,test[u'工作城市匹配'] ,test[u'工作地址距离'] ,test[u'工作地址置信度']  = zip(*test1.apply( work_match,axis=1))

test[u'姓名匹配'],test[u'家庭省份匹配'] ,test[u'家庭城市匹配'] ,test[u'家庭地址距离'] ,test[u'家庭地址置信度']  = zip(*test1.apply( family_match,axis=1))

# test.to_csv(path+'test.csv',encoding='gb2312' )
test.to_csv(path+'test.csv' )
