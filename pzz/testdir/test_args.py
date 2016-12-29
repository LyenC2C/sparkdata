#coding:utf-8

#from pyspark import *
import os
import commands
import sys

def cut_need(path,start,end):
    cmd = 'hadoop fs -ls '+path +' | awk  \'{print $8}\' | grep 201'
    out = commands.getoutput(cmd)
    ls = out.strip().split("\n")
    res = []
    for each in ls:
        day = each.split('/')[-1]
        if int(day) >= int(start) and int(day) <= int(end):
            res.append(each)
    return res
if __name__ == '__main__':
    print cut_need(sys.argv[1],sys.argv[2],sys.argv[3])
