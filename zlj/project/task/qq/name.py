#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')


import re
from jpype import *
import jpype
sys.setrecursionlimit(10000)
vmPath = jpype.getDefaultJVMPath()
jpype.startJVM(vmPath, "-Xms320m", "-Xmx1024m","-mx1024m","-Djava.class.path=/common/segfile/hanlp-1.2.7.jar:")
JDClass = JClass("com.hankcs.hanlp.seg.CRF.CRFSegment")
HJDClass = JClass("com.hankcs.hanlp.HanLP")
HJDClass.setRoot("/common/segfile/")
coreStop=JClass("com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary")
jd = JDClass().enableNameRecognize(True)


for line  in open('/home/zlj/tmp/part.1000'):
    qqnames=line.split()[1:]
    names=[]
    for s in qqnames:
        words=jd.seg(jpype.JString(s.split(':')[0]))
        for i  in words:
            if i.nature=='nz':names.append(i)
    print names