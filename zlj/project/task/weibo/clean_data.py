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

pattern1 = re.compile(r"\[(.*?)\]", re.I|re.X)
pattern2 = re.compile(r"\#(.*?)\#", re.I|re.X)
pattern3 = re.compile(r"\@(.*?)\ ", re.I|re.X)
pattern4 = re.compile(r"(http://|https://)([A-Za-z0-9\./-_%\?\&=:]*)?", re.I)


stopwords=set()
for line in open('/common/stopwords.txt'):
    stopwords.add(line.strip().decode('utf-8'))
# 分词
def cut(line):
    words=jd.seg(jpype.JString(line))
    coreStop.apply(words)
    return [i.word for i  in words if i.word not in stopwords]


# 清洗微博内容
def clean_weibo(line):
    expressions = pattern1.findall(line)
    txt=pattern4.sub('',pattern3.sub('',pattern2.sub('',pattern1.sub('',line))))
    return  [expressions,txt]


# fin = open("/home/wrt/wrt/weibo/negative_label")
fw = open('/mnt/raid2/zlj/weibo1/weibo_mts_20160706_cutsd','w')

fo = open('/mnt/raid2/zlj/weibo1/weibo_mts_20160706.txt')

for line in fo:
    try:
        emo, txt = clean_weibo(line)
        words = cut(txt)
        fw.write(' '.join([i+'_emo' for  i in emo])+' '+' '.join(words)+'\001'+line+'\n')
        # print ' '.join([i+'_emo' for  i in emo])
    except: pass
