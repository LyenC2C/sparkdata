#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
# import rapidjson as json
import re
'''

https://github.com/izisong/new-words-discovery

https://github.com/sycbelief/Digwords

http://www.hankcs.com/nlp/extraction-and-identification-of-mutual-information-about-the-phrase-based-on-information-entropy.html
http://www.matrix67.com/blog/archives/5044
http://www.jianshu.com/p/8d55a3be3f6a

'''
"""
利用候选词序列词频文件，并计算每个候选词的右(左)邻字熵
由于此脚本只负责算词的邻熵，freq_file可以只包含两字及以上的词
"""
# parser = argparse.ArgumentParser()
# parser.add_argument("new_word_discover", help="new_word_discover")
# parser.add_argument("-s", "--separator", help="field separator", default="\t")
# parser.add_argument("-f", "--freq_limit", help="word minimun frequence", default=1, type=int)
# parser.add_argument("-r", "--reverse", help="when freq_file is reversed", action="store_true")
# parser.add_argument("-o", "--output", help="Candidate Sequence Solidification File")
#
# args = parser.parse_args()
#
# src_file, des_file, freq_limit = args.freq_file, args.output, args.freq_limit


sc = SparkContext(appName="new_word")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

fuhao1=',，.。!！?？;；:：\'‘’"”“、(（)）<《>》[【】]{} '.decode('utf-8')
# sentence = re.sub(fuhao1,' ',s.decode('utf-8'))
# ss = sentence.split()
# for i in ss:print i
# 按标点切割分句
def cut_sentence(line):
    sentence = re.sub(fuhao1,' ',line)
    return sentence.split()

# 生成word 字符串
def split_word(line,word_L):
    rs=[]
    size=len(line)
    for i  in xrange(size):
        for j in xrange(2,word_L):
            if i+j<=size:rs.append(line[i:i+j])
    return rs

def datrie_add(word,count,trie):
    i=word
    if i in trie:
        trie[i]=trie[i]+count
    else:trie[i]=count


import datrie
import math
trie = datrie.Trie(ranges=[(u'\u0000',u'\u9FA5')])
trie_r = datrie.Trie(ranges=[(u'\u0000',u'\u9FA5')])

def  calc_solid(word):
    pair=[]
    if len(word)==2:
        word1,word2=word[0],word[1]
        pair.append((word1,word2))
    if len(word)==3:
        word1,word2=word[:1],word[2]
        pair.append((word1,word2))
        word1,word2=word[0],word[1:2]
        pair.append((word1,word2))
    if len(word)==4:
        word1,word2=word[:1],word[2:]
        pair.append((word1,word2))
    ls=[]
    for (k,v) in pair:
        ls.append(math.log(trie[word])-(math.log(trie[k])+math.log(trie[v])))
    return (word,min(ls)) # 取凝固度最小值

def  calc_free(word,word_size):
    right_enty=0
    left_enty=0
    for i in trie.keys(word):
        p=trie[i]*1.0/word_size
        right_enty+=(-p)*math.log(p)
    word_r=word[::-1]
    for i in trie_r.keys(word_r):
        p0=trie_r[i]*1.0/word_size
        left_enty+=p*math.log(p0)
    return (word,(right_enty,left_enty))


path=''
word_L=5
rdd=sc.textFile(path).map(lambda x:cut_sentence(x)).flatMap(lambda x:x).map(lambda x:split_word(x,word_L))

# 过滤低频
limit=100
count_rdd=rdd.flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).filter(lambda x:x[1]>limit)



broadcastVar=sc.broadcast(count_rdd.collectAsMap())

#
# d={}
# for k,v d.items():
#

wordmap = broadcastVar.value
for word,count in wordmap.items():
    word_r=word[::-1]#reverse
    datrie_add(word,count,trie)
    datrie_add(word_r,count,trie_r)

word_size=count_rdd.count()
free_rdd=count_rdd.map(lambda x:calc_free(x[0],word_size))
solid_rdd=count_rdd.map(lambda x:calc_solid(x[0]))

count_rdd.union(free_rdd).union(solid_rdd).groupByKey()
# ls=[
#     u'举个栗子是字举个符串举个',
#     u'这个就有问题了，“ 的” 和 “的地位” 在trie树中， “地位” 也被判断为在trie中，这就不对了'
# ]




# word_size=0
# for line in open():
#     lines=cut_sentence(line)
#     for words in lines:
#         ls=split_word(words,5)
#         word_size+=len(ls)
#         for i in ls:
#             j=i[::-1]
#
#
# wordset=set()
# word_enty={}

# 计算自由度 左右熵



sc.textFile('/user/zlj/temp/tb_wine_title').map(lambda x:(x.split()[0],x)).groupByKey().map(lambda (x,y):list(y)[0]).saveAsTextFile('/user/zlj/temp/tb_wine_title_groupby')