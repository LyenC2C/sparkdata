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

line='7457362	13348886789	{"order_list": [{"receiverName": "佘雨坤", "receiverAddress": "火车南站街道紫薇东路11号（翔宇苑）10栋2单元201", "receiverState": "四川省", "created": "2015-01-13 21:05:37", "buyerNick": "tb8255255_2012", "receiverCity": "成都市", "receiverMobile": "13348886789"}, {"receiverName": "蔡蔡", "receiverAddress": "   十陵街道 双龙社区13217080848", "receiverState": "四川省", "created": "2016-02-17 18:28:03", "buyerNick": "yzr8363780", "receiverCity": "成都市", "receiverMobile": "13348886789"}]}'


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