#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

'''
http://rzcoding.blog.163.com/blog/static/22228101720131019104850984/
'''
# from pyspark import SparkContext
# from pyspark.sql import *
# from pyspark.sql.types import *
# import time
# import rapidjson as json
#
# sc = SparkContext(appName="cmt")
# sqlContext = SQLContext(sc)
# hiveContext = HiveContext(sc)


posdict = [line.strip() for line in open('D:\\data\\review\\posdict.txt', 'r')]
negdict = [line.strip() for line in open('D:\\data\\review\\negdict.txt', 'r')]
mostdict = [line.strip() for line in open('D:\\data\\review\\most.txt', 'r')]
verydict = [line.strip() for line in open('D:\\data\\review\\very.txt', 'r')]
moredict = [line.strip() for line in open('D:\\data\\review\\more.txt', 'r')]
ishdict = [line.strip() for line in open('D:\\data\\review\\ish.txt', 'r')]
insufficientdict =[line.strip() for line in open('D:\\data\\review\\insufficiently.txt', 'r')]
inversedict = [line.strip() for line in open('D:\\data\\review\\inverse.txt', 'r')]


# print negdict


def judgeodd(num):
    if (num/2)*2 == num:
        return 'even'
    else:
        return 'odd'

def sentiment_score(segtmp):
    i = 0 #记录扫描到的词的位置
    a = 0 #记录情感词的位置
    poscount = 0 #积极词的第一次分值
    poscount2 = 0 #积极词反转后的分值
    poscount3 = 0 #积极词的最后分值（包括叹号的分值）
    negcount = 0
    negcount2 = 0
    negcount3 = 0
    for word in segtmp:
        if word in posdict: #判断词语是否是情感词
            poscount += 1
            c = 0
            for w in segtmp[a:i]:  #扫描情感词前的程度词
                if w in mostdict:
                    poscount *= 4.0
                elif w in verydict:
                    poscount *= 3.0
                elif w in moredict:
                    poscount *= 2.0
                elif w in ishdict:
                    poscount /= 2.0
                elif w in insufficientdict:
                    poscount /= 4.0
                elif w in inversedict:
                    c += 1
            if judgeodd(c) == 'odd': #扫描情感词前的否定词数
                poscount *= -1.0
                poscount2 += poscount
                poscount = 0
                poscount3 = poscount + poscount2 + poscount3
                poscount2 = 0
            else:
                poscount3 = poscount + poscount2 + poscount3
                poscount = 0
            a = i + 1 #情感词的位置变化
        elif word in negdict: #消极情感的分析，与上面一致
            negcount += 1
            d = 0
            for w in segtmp[a:i]:
                if w in mostdict:
                    negcount *= 4.0
                elif w in verydict:
                    negcount *= 3.0
                elif w in moredict:
                    negcount *= 2.0
                elif w in ishdict:
                    negcount /= 2.0
                elif w in insufficientdict:
                    negcount /= 4.0
                elif w in inversedict:
                    d += 1
            if judgeodd(d) == 'odd':
                negcount *= -1.0
                negcount2 += negcount
                negcount = 0
                negcount3 = negcount + negcount2 + negcount3
                negcount2 = 0
            else:
                negcount3 = negcount + negcount2 + negcount3
                negcount = 0
            a = i + 1
        # elif word == '！'.decode('utf8') or word == '!'.decode('utf8'): ##判断句子是否有感叹号
        #     for w2 in segtmp[::-1]: #扫描感叹号前的情感词，发现后权值+2，然后退出循环
        #         if w2 in posdict or negdict:
        #             poscount3 += 2
        #             negcount3 += 2
        #             break
        i += 1 #扫描词位置前移

    #以下是防止出现负数的情况
    pos_count = 0
    neg_count = 0
    if poscount3 < 0 and negcount3 > 0:
        neg_count += negcount3 - poscount3
        pos_count = 0
    elif negcount3 < 0 and poscount3 > 0:
        pos_count = poscount3 - negcount3
        neg_count = 0
    elif poscount3 < 0 and negcount3 < 0:
        neg_count = -poscount3
        pos_count = -negcount3
    else:
        pos_count = poscount3
        neg_count = negcount3
    return  [pos_count, neg_count]



# s='我  很 傻 很 蠢'


# print s.split()

fw=open('D:\\workdata\\baijiu_seg_senti','w')
for line in open('D:\\workdata\\baijiu_seg'):
    ls=line.split('\t')
    brand=ls[0].decode('utf-8')
    tt=sentiment_score(ls[1:])
    score=tt[0]-tt[1]
    if  score >1:
        rs= brand+'\t' +'1'
    elif  score <-1:
        rs =brand+'\t' +'-1'
    else:
        rs =brand+'\t' +'0'

    # fw.write(rs+'\n')