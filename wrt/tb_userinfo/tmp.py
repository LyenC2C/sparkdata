#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

for line in open('loc.map.final'):
    id ,loc =line.split()
    print ''.join([ i if len(i)==2 else '0'+i for i in id.split('-')[1:]])+"\' ,\'"+'-'.join(loc.split('-')[1:3])


