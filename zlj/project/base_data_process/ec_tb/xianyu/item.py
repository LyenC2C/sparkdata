#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

s=set()
for line in open(u"E:\\项目\\扶贫完整数据\\标签.txt"):
    for word in line.split(','):
        s.add(word.strip())

for word in s:print word
# def parse_xianyu_item(line):
#     ob=json.loads(line)
#     vip_level=ob.get('vip-level','-')
#     price=ob.get('price','-')
#     price_unit=ob.get('price-unit','-')
#     comments_num=ob.get('comments-num','-')
#     category=ob.get('category','-')
#     grab_time=ob.get('grab-time','-')
#     title=ob.get('title','-')
#     seller=ob.get('seller','-')
#     item_url=ob.get('item-url','-')
#     location=ob.get('location','-')
enumerate

