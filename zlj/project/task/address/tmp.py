#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

# for ob in address:
#     # print type(i)
#     # ob=json.loads('\''+valid_jsontxt(i)+'\'')
#     # prov=ob['abbrname'].replace('省','').replace('自治区','').replace('回族','').replace('维吾尔','').replace('壮族','')
#     # prov_dic[prov]=ob['name']
#     ob1=ob['sub']
#     for w in ob1:
#         print w['name']

# line='7457362	13348886789	{"order_list": [{"receiverName": "佘雨坤", "receiverAddress": "火车南站街道紫薇东路11号（翔宇苑）10栋2单元201", "receiverState": "四川省", "created": "2015-01-13 21:05:37", "buyerNick": "tb8255255_2012", "receiverCity": "成都市", "receiverMobile": "13348886789"}, {"receiverName": "蔡蔡", "receiverAddress": "   十陵街道 双龙社区13217080848", "receiverState": "四川省", "created": "2016-02-17 18:28:03", "buyerNick": "yzr8363780", "receiverCity": "成都市", "receiverMobile": "13348886789"}]}'
#
#

line='3674918	15996928280	{"order_list": [{"receiverName": "孙航", "receiverAddress": "凤城镇锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2016-04-28 17:25:27", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2015-05-20 12:11:47", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2015-05-20 12:12:34", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "中阳里办事处锦绣水岸22栋一单元501", "receiverState": "江苏省", "created": "2015-07-10 12:26:23", "buyerNick": "sunzhi716", "receiverCity": "徐州市", "receiverMobile": "15996928280"}, {"receiverName": "孙航", "receiverAddress": "凤城镇锦绣水岸22栋1-501", "receiverState": "江苏省", "created": "2016-04-17 18:28:19", "buyerNick": "sunhang52848", "receiverCity": "徐州市", "receiverMobile": "15996928280"}]}'
ls= line.split('\t')
import json
rs=[]
for ob  in json.loads(ls[-1])['order_list']:
    rs.append([
        ob['receiverState'],
        ob['receiverCity'],
        ob['receiverAddress'],
        ob['receiverMobile'],
               ])

import requests


head={
    'Host':'restapi.amap.com',
    'Connection':'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'
}

url='http://restapi.amap.com/v3/geocode/geo?key=510cf51a347e0890c99f40370552acd5&address=%E5%9B%9B%E5%B7%9D%E6%88%90%E9%83%BD%E5%8D%81%E9%99%B5%E8%A1%97%E9%81%93%E5%8F%8C%E9%BE%99%E7%A4%BE%E5%8C%BA&output=json'
r = requests.get(url,headers=head,timeout=2)

print r.text
print r.content
