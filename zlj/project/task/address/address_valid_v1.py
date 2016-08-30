#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import re


import json

prov_dic={}
city_dic={}
xian_dic={}

def valid_jsontxt(content):
    res = str(content)
    if type(content) == type(u""):
        res = content.encode("utf-8")
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

# for ob in address:
#     prov=ob['abbrname'].replace('省','').replace('自治区','').replace('回族','').replace('维吾尔','').replace('壮族','')
#     prov_dic[prov]=ob['name']
#     citys=ob['sub']
#     for city_ob in citys:
#         city_dic[city_ob['name'].replace('市','')]=city_ob['name']
#         xians=city_ob['sub']
#         for xian_ob in xians:
#             xian_dic[xian_ob['name'].replace('县','').replace('区','')]=xian_ob['name']

# 地址
def log(w):
     if type(w)==type([]) or  type(w)==type(()):
         print  '\t'.join(w)
     else :print w
    # return  ""

import copy

# print prov_dic['四川'] ,'----------'
# def check_prov(line ,words,address_ls):
#     words_tmp = copy.deepcopy(words)
#     if '省' in line or '自治区' in line :
#         for index,prov in enumerate(address_ls):
#              if '省' in prov:
#                  return prov, words,address_ls[index:]
#     else:
#         for index,prov  in enumerate(words_tmp) :
#             if prov_dic.has_key(prov):
#                return prov,words[index:],address_ls
#     return "",words,address_ls
# def check_city(line ,words,address_ls):
#     words_tmp = copy.deepcopy(words)
#     if '市' in line :
#         for index,city in enumerate(address_ls):
#              if '市' in city:
#                  return city, words,address_ls[index:]
#     else:
#         for index,city  in enumerate(words) :
#             if city_dic.has_key(city):
#                return city,words[index:],address_ls
#     return "",words,address_ls
#
# def check_xian_qu(line ,words,address_ls):
#     words_tmp = copy.deepcopy(words)
#     if '县' in line or '区' in line:
#         for index,xian in enumerate(address_ls):
#              if '县' in xian  or  '区' in xian:
#                  return xian, words,address_ls[index:]
#     else:
#         for index,xian  in enumerate(words_tmp) :
#             if xian_dic.has_key(xian):
#                return xian,words[index:],address_ls
#     return "",words[index:],address_ls

from Levenshtein import *


import jieba
# 提取地址
def extract(address):
    words='\001'.join(jieba.cut(address)).encode('utf-8').split('\001')
    log(words)
    address_ls=seg.mainAlgorithm_String(address).split('|')
    # address_ls=[]
    log(address_ls)
    line=address.replace('社区','').replace('小区','')
    prov,words,address_ls=check_prov(line,words,address_ls)
    log(prov)
    log(words)
    log(address_ls)
    city,words,address_ls=check_city(line,words,address_ls)
    log(city)
    log(words)
    log(address_ls)
    xian,words,address_ls=check_xian_qu(line,words,address_ls)
    log(xian)
    log(words.append(""))
    log(address_ls.append(""))
    city=city.replace(prov,'')
    xian=xian.replace(prov,'').replace(city,'')

    address_ls=seg.mainAlgorithm_String(address.replace(prov,'').replace(city,'').replace(xian,''))
    return [prov_dic.get(prov,prov),
            city_dic.get(city,city),
            xian_dic.get(xian,xian),
            ''.join(address_ls)]

import Levenshtein
# 权重设计
weght=[0.3,0.3,0,0.4]

# 计算相似度
def sim(ad_real,ad_test):
    score=0
    for index,item in enumerate(zip(ad_real[:-1],ad_test[:-1])):
        if len(item[0])>1 and len(item[1])>1  and item[0]==item[1]: score=score+weght[index]
    dis=Levenshtein.distance(ad_real[-1],ad_test[-1])
    score=score+weght[-1]*1/(dis+1)
    return score

# line ='四川省成都市十陵街道双龙社区'
# ad_real=extract('四川省成都市十陵街道双龙社区')
# ad_test=extract('四川成都市十陵街道双龙社区')
# print sim(ad_real,ad_test)
import json

# 解析淘宝地址
def taobao_address(address):
    ls= address.split('\t')
    rs=[]
    for ob  in json.loads(ls[-1])['order_list']:
        rs.append([
            ob['receiverState'],
            ob['receiverCity'],
            ob['receiverAddress'],
            ob['receiverName'],
            ob['receiverMobile'],
                   ])
    return [ls[0],ls[1],rs]

name_state = {
   0:'姓名',
   1:'省份',
   2:'城市',
   3:'详细地址'
         }


def match_info(v):
    if type(v)==type(True):
        if v==True :return '匹配'
        else :return '不匹配'
    if type(v)==type(1.0):
        return '置信度:'+ str(v)



head={
    'Host':'restapi.amap.com',
    'Connection':'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'
}


import requests
def address_format(address):
    url='http://restapi.amap.com/v3/geocode/geo?key=510cf51a347e0890c99f40370552acd5&address='+address +'&output=json'
    r=requests.get(url,headers=head,timeout=2)
    ob=json.loads(r.text)
    if type(ob)!=type({}):return (-1,'请求异常')
    if ob.get('status',-1)!='1':return (-1,'请求异常')
    geocodes=ob.get('geocodes',{})[0]
    province=valid_jsontxt(geocodes.get('province',''))
    city=valid_jsontxt(geocodes.get('city',''))
    district=valid_jsontxt(geocodes.get('district',''))
    return (1,(province,city,district,address.replace(province,'').replace(city,'').replace(district,'')))


# 匹配

line='3674918	15996928280	{"order_list": [{"receiverName": "孙航", "receiverAddress": "凤城镇锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2016-04-28 17:25:27", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2015-05-20 12:11:47", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2015-05-20 12:12:34", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "中阳里办事处锦绣水岸22栋一单元501", "receiverState": "江苏省", "created": "2015-07-10 12:26:23", "buyerNick": "sunzhi716", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "凤城镇锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2016-04-17 18:28:19", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}]}'
def  match(tel,name,address):
    ob=address_format(address)
    if  ob[0]==-1 :return  (-1,'查询无结果')
    else :
        prov,city,xian,other=ob[1]
    ls=[]
    # data=taobao_address(tel)
    data=json.loads(line.split('\t')[-1])
    if len(line)==0:return  (-1,'查询无结果')
    for index, item in enumerate(data['order_list']):
        receiverState=valid_jsontxt(item.get('receiverState','-'))
        receiverCity=valid_jsontxt(item.get('receiverCity','-'))
        receiverAddress=valid_jsontxt(item.get('receiverAddress','-'))
        receiverName=valid_jsontxt(item.get('receiverName','-'))
        receiverMobile=valid_jsontxt(item.get('receiverMobile','-'))
        ls.append((name in receiverName, prov in receiverState,city in receiverCity ,
          1.0/(Levenshtein.distance(other,receiverAddress)+1) )
         )
    top_index=sorted([(v,index) for index, v in enumerate(map(sum,ls))])[-1]
    rs=[]
    # (receiverState,receiverCity,receiverAddress,receiverName,receiverMobile)
    state=ls[top_index[-1]]
    for index, v in enumerate(state):
        # print name_state[index],match_info(v)
        rs.append(name_state[index]+match_info(v))
    return rs


rs= match('15996928280','孙航','江苏省徐州市凤城镇锦绣水岸')
print '\t'.join(rs)


rs= match('15996928280','孙航sd','江苏省徐州市凤城镇锦绣水岸')
print '\t'.join(rs)

rs= match('15996928280','孙航','徐州市凤城镇锦绣水岸')
print '\t'.join(rs)


# ad_real=extract('四川省成都市十陵街道双龙社区')
# ad_test=extract('十陵街道双龙社区')
# print ad_real ,ad_test
# print sim(ad_real,ad_test)
#
# ad_real=extract('四川省峨眉山市十陵街道双龙社区')
# ad_test=extract('十陵街道双龙社区')
# log(ad_real)
# log(ad_test)
# print sim(ad_real,ad_test)
# print '\t'.join(extract('四川成都市十陵街道双龙社区'))
# print '\t'.join(extract('四川成都龙泉驿区十陵街道双龙社区'))