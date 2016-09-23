#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')
#  商务地址验证

from math import radians, cos, sin, asin, sqrt
head={
    'Host':'restapi.amap.com',
    'Connection':'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'
}
def valid_jsontxt(content):
    res = str(content)
    if type(content) == type(u""):
        res = content.encode("utf-8")
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

import json
import requests
def haversine(lon1, lat1, lon2, lat2): # 经度1，纬度1，经度2，纬度2 （十进制度数）
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # 地球平均半径，单位为公里
    return c * r
# 地址格式化
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
def address_gps(address):
    url='http://restapi.amap.com/v3/geocode/geo?j=json&key=510cf51a347e0890c99f40370552acd5&address='+address
    r=requests.get(url,headers=head,timeout=2)
    ob=json.loads(r.text)
    if type(ob)!=type({}):return (-1,'请求异常')
    if ob.get('status',-1)!='1':return (-1,'请求异常')
    rs=ob.get('geocodes',[{'location':'0,0'}])[0].get('location').split(',')
    return [float(i) for i in rs]

f=open(u'E:\\项目\\商务\\地址验证')

ori_info={}
for line in f:
    name,id,tel,address=line.split()
    ori_info[tel]=(name.strip(),id.strip(),tel.strip(),address.strip())

f1=open(u'E:\\项目\\商务\\地址验证_tb')

def fun(x):
    if x==True:return '匹配'
    if x==False:return '不匹配'
for line in f1:
    if len(line.split())<6:continue
    tb_tel,tb_name,tb_add_detail,tb_prov,tb_nick,tb_city=line.split()[:6]
    if ori_info.has_key(tb_tel):
        # print line
        name,id,tel,address=ori_info[tb_tel]
        match_prov,match_city,district,match_address_detail=address_format(address)[1]
        taobao_gps=address_gps(tb_prov+tb_city+tb_add_detail)
        match_gps=address_gps(address)
        distance=haversine(match_gps[0],match_gps[1],taobao_gps[0],taobao_gps[1])
        confidence=(100-distance*2)
        confidence= confidence if confidence>0 else 0
        # print match_prov ,match_city,match_address_detail ,':' ,tb_prov,tb_city,tb_add_detail
        rs=[ name,id,tel,  'name:',fun(tb_name in name), 'province:', fun(tb_prov in match_prov),  'city:', fun(match_city in tb_city),'distance:',round(distance,3)]
        print '\t'.join([ str(i) for i in rs ])
