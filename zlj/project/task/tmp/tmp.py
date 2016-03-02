#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')


end_flag=0
buf=''
# for line in open("E:\\work\\xinzhengxin\\oulaiya_ xinghao_1.txt"):
#     if 'END' in line:
#         print buf.decode('utf-8')
#         buf=''
#         end_flag=0
#         buf=line.strip()
#     else: buf=buf+'  '+line.strip()

map={}
for line in open("E:\\work\\xinzhengxin\oulaiya.txt"):
    k,v=line.split()
    # print type(v)
    # print v
    map[v]=k
# print map

print(len(map.items()))

for line in open("E:\\work\\xinzhengxin\\oulaiya_ xinghao_1_dealclean.txt"):
    ls=line.split()

    # print ls
    if(len(ls)!=1):
        k=ls[0]
        # print k
        v=ls[1:]
        if map.has_key(k.decode('utf-8').encode('utf-8')):print map[k],k.decode('utf-8')," ".join(v).decode('utf-8')
