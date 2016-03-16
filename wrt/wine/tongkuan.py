__author__ = 'wrt'
#coding:utf-8
import sys
import copy
import math
from pyspark import SparkContext

sc = SparkContext(appName="tongkuan")

def Q2B(uchar):
	inside_code=ord(uchar)
	if inside_code==0x3000:
		inside_code=0x0020
	else:
		inside_code-=0xfee0
	if inside_code<0x0020 or inside_code>0x7e:
		return uchar
	return unichr(inside_code)
def stringQ2B(ustring):
	return "".join([Q2B(uchar) for uchar in ustring])
def uniform(ustring):
	return stringQ2B(ustring).lower()

def pipei(list1,list2):
    n = 0.0
    words1 = copy.deepcopy(list1[0])
    words2 = copy.deepcopy(list2[0])
    values1 = copy.deepcopy(list1[1])
    values2 = copy.deepcopy(list2[1])
    # l1 = sum(values1)
    # l2 = sum(values2)
    # l1 = 0
    # l2 = 0
    l1 = sum([i*i for i in values1])
    l2 = sum([i*i for i in values2])
    # for ln in values1:
    #     l1 += ln*ln
    # for ln in values2:
    #     l2 == ln*ln
    #l1 = math.sqrt(l1)
    for i in range(len(words1)):
        if words1[i] in words2:
            j = words2.index(words1[i])
            n += values1[i]*values2[j]
            del words2[j]   #删除匹配到的词和与之对应的权值
            del values2[j]
    if math.sqrt(l1)*math.sqrt(l2) == 0:
        return 0.0
    else:
        return float(n/(math.sqrt(l1)*math.sqrt(l2)))
def f1(line):
    ss = line.strip().split('\t')
    if len(ss) != 3: return None
    if ss[1].strip() == "": return None
    item_id = ss[0]
    brand = ss[1]
    title = ss[2].split("\001")#[1:]
    words = []
    values = []
    for ln in title:
        if len(ln.split("_")) != 2: return None
        word = uniform(ln.split("_")[0]) #所有字母变小写
        value = float(ln.split("_")[1]) #匹配权值
        words.append(word)
        values.append(value)
    return (brand,[item_id,words,values])

def f2(x,y):
    brand_list = y
    l = len(brand_list)
    result = []
    for i in range(l):
        for j in range(i+1,l):
            k1 = brand_list[i]
            k2 = brand_list[j]
            pipei_value = pipei(k1[1:],k2[1:]) #([words,values],[words,values])
            title1 = "".join(k1[1])
            title2 = "".join(k2[1])
            if float(pipei_value) > 0.9:
                result.append(x + "\t" + k1[0] + "\t" + title1 + "\t" + k2[0] + "\t" + title2 + "\t" + str(pipei_value))
    return result


rdd = sc.textFile("/user/zlj/temp/tb_wine_title_all_tfidf_value").map(lambda x:f1(x)).filter(lambda x:x!=None)
rdd2 = rdd.groupByKey().mapValues(list).flatMap(lambda (x,y):f2(x,y))
rdd2.saveAsTextFile('/user/zlj/temp/tb_all_tfidf_pipei_result_09')

#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 tongkuan.py