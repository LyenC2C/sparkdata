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


from Levenshtein import *

# 提取地址

import Levenshtein

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

# 状态
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
    if type(v)!=type(True):
        return '置信度:'+ str(v)


# 请求head
head={
    'Host':'restapi.amap.com',
    'Connection':'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'
}


import requests
# 地址格式化
def address_format(address):
    url='http://restapi.amap.com/v3/geocode/geo?key=510cf51a347e0890c99f40370552acd5&address='+address +'&output=json'
    r=requests.get(url,headers=head,timeout=2)
    ob=json.loads(r.text)
    if type(ob)!=type({}):return (-1,'请求异常')
    if ob.get('status',-1)!='1':return (-1,'请求异常')
    geocodes=ob.get('geocodes',{})[0]
    province=valid_jsontxt(geocodes.get('province',''))
    city=valid_jsontxt(geocodes.get('city','')).replace('市','')
    district=valid_jsontxt(geocodes.get('district',''))
    return (1,(province,city,district,address.replace(province,'').replace(city,'').replace(district,'')))

# 高德地址gps请求
def address_gps(address):
    url='http://restapi.amap.com/v3/geocode/geo?j=json&key=510cf51a347e0890c99f40370552acd5&address='+address
    r=requests.get(url,headers=head,timeout=2)
    ob=json.loads(r.text)
    if type(ob)!=type({}):return (-1,'请求异常')
    if ob.get('status',-1)!='1':return (-1,'请求异常')
    rs=ob.get('geocodes',[{'location':'0,0'}])[0].get('location').split(',')
    return [float(i) for i in rs]

# 匹配
from math import radians, cos, sin, asin, sqrt

# gps距离
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


# 测试数据
# line='3674918	15996928280	{"order_list": [{"receiverName": "孙航", "receiverAddress": "凤城镇锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2016-04-28 17:25:27", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2015-05-20 12:11:47", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2015-05-20 12:12:34", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "中阳里办事处锦绣水岸22栋一单元501", "receiverState": "江苏省", "created": "2015-07-10 12:26:23", "buyerNick": "sunzhi716", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "凤城镇锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2016-04-17 18:28:19", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}]}'

# 匹配
def  match(tel,name,address,line):
    ob=address_format(address)
    if  ob[0]==-1 :return  (-1,'查询无结果')
    else :
        prov,city,xian,other=ob[1]

    match_gps=address_gps(address)
    ls=[]
    # data=taobao_address(tel)
    data=json.loads(line)
    if len(line)==0:return  (-1,'查询无结果')
    for index, item in enumerate(data['order_list']):
        receiverState=valid_jsontxt(item.get('receiverState','-'))
        receiverCity=valid_jsontxt(item.get('receiverCity','-')).replace('市','')
        receiverAddress=valid_jsontxt(item.get('receiverAddress','-'))
        receiverName=valid_jsontxt(item.get('receiverName','-'))
        receiverMobile=valid_jsontxt(item.get('receiverMobile','-'))
        ls.append([name in receiverName, prov in receiverState,city in receiverCity ,
          1.0/(Levenshtein.distance(other,receiverAddress)+1) ]
         )
    top_index=sorted([(v,index) for index, v in enumerate(map(sum,ls))])[-1]
    rs=[]
    info=data['order_list'][top_index[-1]]

    print info.get('receiverState') ,info.get('receiverCity') ,info.get('receiverAddress')
    taobao_gps=address_gps(valid_jsontxt(info.get('receiverState','-')+info.get('receiverCity','-')+info.get('receiverAddress','-')))
    distance=haversine(match_gps[0],match_gps[1],taobao_gps[0],taobao_gps[1])
    confidence=(100-distance*2)
    confidence= confidence if confidence>0 else 0
    state=ls[top_index[-1]]
    state[-1]=confidence
    for index, v in enumerate(state):
        # print name_state[index],match_info(v)
        print index,v ,name_state[index],match_info(v)
        rs.append(name_state[index]+match_info(v))
    rs.append('距离（公里）:'+str(distance))
    return rs


lines='''
{"mobile": "15327225486", "order_list": [{"receiverName": "张胜", "receiverAddress": "白沙洲街办事处长江紫都二期15栋1单元501室", "created": "2016-09-07 06:02:39", "receiverState": "湖北省", "buyerNick": "静思晨语lcl", "receiverCity": "武汉市", "receiverMobile": "15327225486"}]}
{"mobile": "13986062966", "order_list": [{"receiverName": "刘妍", "receiverAddress": "古北黄金城道919号", "created": "2015-12-21 11:18:34", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘妍", "receiverAddress": "古北黄金城道919号", "created": "2015-12-21 11:18:34", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘妍", "receiverAddress": "古北黄金城道919号", "created": "2015-12-21 11:18:34", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴小枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-05-22 14:18:18", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴小枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-05-22 14:18:18", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴小枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-05-22 14:18:18", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴小枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-05-22 14:18:18", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴小枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-07-17 21:26:51", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘小妍", "receiverAddress": "虹桥镇虹梅路3329号金泉苑3号楼602室", "created": "2015-10-27 14:12:20", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘小妍", "receiverAddress": "虹桥镇虹梅路3329号金泉苑3号楼602室", "created": "2015-10-27 14:12:20", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘小妍", "receiverAddress": "虹桥镇虹梅路3329号金泉苑3号楼602室", "created": "2015-10-27 14:12:20", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘小妍", "receiverAddress": "虹桥镇虹梅路3329号金泉苑3号楼602室", "created": "2016-01-05 14:55:20", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘小妍", "receiverAddress": "虹桥镇虹梅路3329号金泉苑3号楼602室", "created": "2016-01-05 14:55:20", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘小妍", "receiverAddress": "虹桥镇虹梅路3329号金泉苑3号楼602室", "created": "2016-01-05 14:55:20", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘小妍", "receiverAddress": "虹桥镇虹梅路3329号金泉苑3号楼602室", "created": "2016-01-05 14:55:20", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘小妍", "receiverAddress": "虹桥镇虹梅路3329号金泉苑3号楼602室", "created": "2016-01-05 14:55:20", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘小妍", "receiverAddress": "虹桥镇虹梅路3329号金泉苑3号楼602室", "created": "2016-02-17 19:40:46", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "刘小妍", "receiverAddress": "虹桥镇虹梅路3329号金泉苑3号楼602室", "created": "2016-02-17 19:40:46", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴小枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-07-07 14:35:34", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴小枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-07-07 14:35:34", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴小枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-07-07 14:35:34", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴小枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-07-07 14:35:34", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴逸枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-03-30 21:14:26", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴逸枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-03-19 20:49:04", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}, {"receiverName": "吴逸枫", "receiverAddress": "锦屏路71弄7号403室", "created": "2015-03-19 20:49:04", "receiverState": "上海", "buyerNick": "xiaoyan_up", "receiverCity": "上海市", "receiverMobile": "13986062966"}]}
{"mobile": "18628087527", "order_list": [{"receiverName": "卢烨", "receiverAddress": "金耀路158号万科金域西岭4栋速递易", "created": "2015-04-29 18:13:13", "receiverState": "四川省", "buyerNick": "luye_luye", "receiverCity": "成都市", "receiverMobile": "18628087527"}, {"receiverName": "卢烨", "receiverAddress": "金耀路158号万科金域西岭4栋速递易", "created": "2015-04-29 18:13:13", "receiverState": "四川省", "buyerNick": "luye_luye", "receiverCity": "成都市", "receiverMobile": "18628087527"}, {"receiverName": "卢烨", "receiverAddress": "金耀路158号万科金域西岭4栋速递易", "created": "2016-01-15 13:14:41", "receiverState": "四川省", "buyerNick": "luye_luye", "receiverCity": "成都市", "receiverMobile": "18628087527"}, {"receiverName": "卢烨", "receiverAddress": "金耀路158号万科金域西岭4栋速递易", "created": "2016-08-05 23:13:24", "receiverState": "四川省", "buyerNick": "luye_luye", "receiverCity": "成都市", "receiverMobile": "18628087527"}, {"receiverName": "卢烨", "receiverAddress": "金耀路158号万科金域西岭4栋速递易", "created": "2016-08-05 23:13:24", "receiverState": "四川省", "buyerNick": "luye_luye", "receiverCity": "成都市", "receiverMobile": "18628087527"}, {"receiverName": "卢烨", "receiverAddress": "金耀路158号万科金域西岭4栋速递易", "created": "2016-08-05 23:13:24", "receiverState": "四川省", "buyerNick": "luye_luye", "receiverCity": "成都市", "receiverMobile": "18628087527"}, {"receiverName": "卢烨", "receiverAddress": "金耀路158号万科金域西岭4栋速递易", "created": "2016-08-05 23:13:24", "receiverState": "四川省", "buyerNick": "luye_luye", "receiverCity": "成都市", "receiverMobile": "18628087527"}, {"receiverName": "卢烨", "receiverAddress": "金耀路158号万科金域西岭4栋速递易", "created": "2016-08-26 13:38:45", "receiverState": "四川省", "buyerNick": "luye_luye", "receiverCity": "成都市", "receiverMobile": "18628087527"}, {"receiverName": "卢烨", "receiverAddress": "金耀路158号万科金域西岭4栋速递易", "created": "2016-08-26 13:39:24", "receiverState": "四川省", "buyerNick": "luye_luye", "receiverCity": "成都市", "receiverMobile": "18628087527"}]}
'''

origin_info='''
15327225486	湖北省武汉市武昌区白沙洲堤中街
15527640235	北京市朝阳区君天大厦6002
13554381790	上海市浦东新区青桐路333号
15717170155	湖北省武汉市武昌区中华路
13986062966	湖北省武汉市洪山区书城路
13367267269	湖北省武汉市硚口区汉正街
13581938600	北京市朝阳区通惠河畔产业园1131号
13905184869	江苏省南京市中央路100号508
15204797079	湖南省长沙市岳麓区麓山南路932号
13665136578	北京市朝阳区北花园小区
15041391604	辽宁省抚顺市望花区辽宁石油化工大学
15102479828	辽宁省辽阳市宏伟区
18612937939	吉林省延吉二道白河镇池北区
18601092921	上海市杨浦区四平路中天大厦
18628087527	四川省成都市青羊区提督街99号
18515668439	北京市海淀区清华大学
'''

'''
本地测试
'''
def test_loc():
    tb_info=[i for i in lines.split('\n') if  'order_list' in  i ]
    test=[i for i in  origin_info.strip().split('\n') if len(i)>1 ]
    for i in  test:
        tel,address =i.strip().split()
        if tel in lines:
            line=[i for i in tb_info  if tel in i][0]
            rs= match(tel ,'-',address,line)
            if rs!=None :print  tel,address, '\t'.join(rs)



test_loc()
# rs= match('15996928280','孙航','江苏省徐州市凤城镇锦绣水岸')
# print '\t'.join(rs)
#
#
# rs= match('15996928280','孙航sd','江苏省徐州市凤城镇锦绣水岸')
# print '\t'.join(rs)
#
# rs= match('15996928280','孙航','徐州市凤城镇锦绣水岸')
# print '\t'.join(rs)

# print '\t'.join(address_format('上海市松江区')[-1])
# print '\t'.join(address_format('上海松江区')[-1])