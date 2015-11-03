__author__ = 'zlj'
import re
# path='/home/zlj/data/tbcdm_cat.txt'
path='/home/zlj/data/temp1.txt'
j=0
for line in open(path):
    # p=re.compile(r'.+?,|\".+?\"')
    # p=re.compile(r'.+?,|\".+?\"')
    # s=p.findall(line)
    s=line.strip().split('kousu')
    if  len(s)!=34:
        j=j+1
        continue
    # print line
    print   '\001'.join([i.replace('\"','').replace(',','') for i in s])

    # print j
    # print type(s)
    # print s
    # for v in s:
    #     print v
    # print '\n'
    # value=line.split('",')
    # list=[]
    # for i in value:
    #     list.extend(i.split(','))
    # # print len(value)
    # if(len(value)!=36):
    #     print len(value),line

