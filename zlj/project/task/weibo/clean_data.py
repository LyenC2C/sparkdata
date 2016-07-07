#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import re

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from jpype import *
import jpype
# sys.setrecursionlimit(10000)
# vmPath = jpype.getDefaultJVMPath()
# jpype.startJVM(vmPath, "-Xms320m", "-Xmx1024m","-mx1024m","-Djava.class.path=/home/hadoop/common/segfile/hanlp-1.2.7.jar:")
# JDClass = JClass("com.hankcs.hanlp.seg.CRF.CRFSegment")
# HJDClass = JClass("com.hankcs.hanlp.HanLP")
# HJDClass.setRoot("/home/hadoop/common/segfile/")
# jd = JDClass().enableNameRecognize(True)



def  cut(line):
    return jd.seg(jpype.JString(line))
# s="努西娜 2015秋冬平底短靴女真皮厚底靴子马"
# print jd.seg((s))
#print jd.seg(jpype.JString(s))

def dealHtmlTags(html):
    '''''
    去掉html标签
    '''
    from HTMLParser import HTMLParser
    html=html.strip()
    html=html.strip("\n")
    result=[]
    parse=HTMLParser()
    parse.handle_data=result.append
    parse.feed(html)
    parse.close()
    return "".join(result)


def dealUrl(text):
    '''''
    去掉微博信息中的url地址
    '''
    # return re.sub('''''http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*,]|(?:%[0-9a-fA-F][0-9a-fA-F]))+''', '',text)
    print re.sub('''http[s]?://(?:[a-zA-Z]|[$-_@.&+]|[!*,]|(?:%[0-9a-fA-F][0-9a-fA-F]))+''', '',text)
    return re.sub('''http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*,]|(?:%[0-9a-fA-F][0-9a-fA-F]))+''', '',text)


if __name__== "__main__":
    html = """  接下来一年，我希望在<SPAN style="COLOR: red">惠普</SPAN>电脑看到更人性化，各科技化的东西，更能提升视觉享受的东西。 地址：<A title=http://t.cn/8kUAX2z href="http://t.cn/8kUAX2z" target=_blank suda-data="key=tblog_search_v4.1&value=weibo_feed_url" :3651215114310513>http://t.cn/8kUAX2z<SPAN class=feedico_active></SPAN></A>
    """
    # x=re.match('''[A-Za-z0-9_\-\u4e00-\u9fa5]+''',' ')

    for i in re.match('\[(.*?)\]','[不想上班][不想上班] #来自星星的你的红包#最暖心 都是些什么鬼！[笑cry]'):
        print i
    # print x.string
    # for i in
    #     print i
    # html='我在美拍拍了段视频，欢迎围观！点此播放>>http://t.cn/Rw8mAW1（通过 #美拍# 拍摄）'
    html =  dealHtmlTags(html)
    print dealUrl(html)