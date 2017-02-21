#coding:utf-8
'''
闲鱼数据url中提取userid
'''
from urlparse import urlparse
url = "https://h5.m.taobao.com/2shou/pd/rearesulterifyUrl.html?userId=296519963&isVerify=0"
params = urlparse(url).query

kv = {}
for key_value in params.split('&'):
    key = key_value.split('=')[0]
    value = key_value.split('=')[1]
    kv.setdefault(key, value)

# print kv.get("userId")

print

'''
max [[],[],[]] -> []
max ((),(),()) -> ()
'''
from operator import itemgetter
li = []

a = ['a', 15]
c = ['c', 9]
b = ['b', 12]

li.append(a)
li.append(b)
li.append(c)
# ls = max(li, key=lambda a: a[1])
ls = max(li, key=itemgetter(-1))
# print ls[0],ls[1]
'''
sorted
'''
li_sorted = sorted(li,key=lambda a:a[-1],reverse=True)
'''
for each in li_sorted:
    print each[0],each[1]
'''

#pythonic
1.
dic = {"lyen":"cc","lc":"22"}
if "lyen" in dic:print "yes"
2.
dictionary = {}
dictionary.setdefault("key", []).append("list_item")
for k,v in dictionary.iteritems():
    print k,v
3.
3.1
d = {}
if 'a' not in d:
    d['a'] = 100
d['a'] += 10
3.2
from collections import defaultdict
d = defaultdict(lambda: 100)
d['a'] += 10
4
4.1
#items() 返回的是一个 list，list 在迭代的时候会预先把所有的元素加载到内存
ds = {i: i * 2 for i in xrange(10000000)}
#for key, value in d.items():
#    print("{0} = {1}".format(key, value))
4.2
#而iteritem()返回的一个迭代器(iterators)，迭代器在迭代的时候，迭代元素逐个的生成。
# for key, value in ds.iteritems():
#     print("{0} = {1}".format(key, value))
5
#合并多个字典
5.1
#items()方法在python2.7中返回的是列表对象，两个列表相加得到一个新的列表，这样内存中存在3个列表对象，
x = {'a': 1, 'b': 2}
y = {'b': 3, 'c': 4}
z = dict(x.items() + y.items())
5.2
#优化
x = {'a': 1, 'b': 2}
y = {'b': 3, 'c': 4}
z = x.copy()
z.update(y)
5.3
def merge_dicts(*dict_args):
    '''
   可以接收1个或多个字典参数
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result
#z = merge_dicts(a, b, c, d)
'''
try:
    value = collection[key]
except KeyError:
    return key_not_found(key)
else:
    return handle_value(value)
#No:
try:
    # Too broad!
    return handle_value(collection[key])
except KeyError:
    # Will also catch KeyError raised by handle_value()
    return key_not_found(key)
'''

'''
Yes: if foo.startswith('bar'):
No:  if foo[:3] == 'bar':

Yes: if isinstance(obj, int):
No:  if type(obj) is type(1):

For sequences, (strings, lists, tuples), use the fact that empty sequences are false.
Yes: if not seq:
     if seq:
No: if len(seq):
    if not len(seq):

Yes:   if greeting:
No:    if greeting == True:
Worse: if greeting is True:

[ item for item in os.listdir(os.path.expanduser('~')) if os.path.isfile(item) ]
{ item: os.path.realpath(item) for item in os.listdir(os.path.expanduser('~')) if os.path.isdir(item) }

with open(r'C:\misc\data.txt') as myfile:
    for line in myfile:
        ...use line here..
'''
'''
b = 2 if a > 2 else 1
b = False if a > 1 else True #A ? B : C
'''


def read_file(fpath):
    BLOCK_SIZE = 1024
    with open(fpath, 'rb') as f:
        while True:
            block = f.read(BLOCK_SIZE)
            if block:
                yield block

#for i in read_file("/home/lyen/airflow useage.md"):
#   print i
str = "手机:4254,电话:081"
str_1 ="3、“郑某”开设赌场判处缓刑等等。 执业以来，刑事案件判处缓刑率达到15%左右。手机：13616776886电话：0577-86861956QQ：379619056传真：86862557 邮箱：379619056@qq.com联系地址：温州市龙湾区海滨街道富海路191号 "

import re
phones = filter(lambda a: len(a) >= 11,re.findall(r"\d+[-]?\d*",str_1))
print len(phones)


'''
银行电话号码及名字
'''
# bankinfo ="(86 20)3680 8800,传真:(86 20)3685 5037 手机：13616776886 电话：0577-86861956,(23) 6366 552".replace(' ','').replace('（','(').replace('）',')')
# b = "（021）66262773（021）66262001".replace(' ','').replace(' ','').replace('（','(').replace('）',')')
# c = "对公：0577-57760111（服务时间：周一至周五，假日除外08：30-11：30；13：30-17：00）对私：0577-57760111（服务时间：周一至周日08：30-17：00）大堂：0577-57762735（服务时间：周一至周日08：30-17：00）"
#
# numbers = re.findall(r"[(|]?\d+[)|）]?[-]?\d*",c)
# for phone in numbers:
#     if phone.startswith('('):
#         phone_sub= re.findall(r"\d+?\d*",phone)
#         if phone_sub[0].startswith('86'):
#             phone = phone_sub[0].replace('86','')+'-'+phone_sub[1]
#         else:
#             phone = phone_sub[0]+'-'+phone_sub[1]
#     if len(phone) >5:
#         print phone

raw = '{\"code\":\"00\",\"userName\":\"翟波\",\"credentialCode\":\"512929197310280374\",\"userId\":\"AEF37B1930814EED878C4EBF536410AA\",\"msg\":\"验证通过！\"}'
import json
print json.loads(raw.replace('\\','')).get("userName")


