#coding:utf-8
__author__ = 'zlj'
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
sc = SparkContext(appName="impr",conf=conf)
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
'''
http://www.cnblogs.com/siegfang/p/3455160.html
'''
#
schema1 = StructType([
    StructField("item_id", StringType(), True),
    StructField("feed_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("feed", StringType(), True),
    StructField("impr", StringType(), True),
    StructField("neg_pos", IntegerType(), True),
    StructField("impr_c", StringType(), True)
    ])


def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content

def f_coding(x):
    if type(x) == type(""):
        return x.decode("utf-8")
    else:
        return x


degree_line=u'''更 2
更_2
更加_2
很_2
极_2
几乎_2
蛮_2
太_2
挺_2
越_2
越发_2
非常_2
灰常_2
倍儿_2_
有些_1
略微_1
过于_1
或许_1
了点_1
略微_1
偏_1
稍_1
稍微_1
免强_1
勉强_1
也许_1
有点_1
有点儿_1
有一点_1
有一点点_1_
十足_3
一如既往_3
very_3
超_3
超常_3
超乎寻常_3
超级_3
彻底_3
出乎寻常_3
非比寻常_3
非常_3
非同寻常_3
分外_3
格外_3
极其_3
绝对_3
十分_3
特别_3
完全_3
尤其_3
最_3
不是一般_3
'''

tag_f=set(u'mbceopquyzrw')


def filter(tag):
    if tag in tag_f:
        return False
    else: return True
# emo_set=set()
neg_set=set()
degree_set=set()

for kv in degree_line.split():
    lv=kv.strip().split('_')
    if len(lv)!=2:continue
    k,v =lv
    degree_set.add(k)

neg_line=u"不是 不太 不能 不可以 没有 不 不行 木有 没  未 别 莫 勿 不够 不必 甭 不曾 不怎么 不如 无 不是 并未 不太 绝不 谈不上 看不出 达不到 并非 从不 从没 毫不 不肯 有待 无法 没法 毫无 没有什么 没什么"

for k in neg_line.split():neg_set.add(k)

emo_rdd=sc.textFile('/user/zlj/data/emo_1').collect()
neg_set_bc=sc.broadcast(emo_rdd)
emo_set=set(neg_set_bc.value)
# for v in neg_set_bc:emo_set.add(v)
# for line in open('D:\\data-emo_1'):
#     emo_set.add(line.strip().decode('utf-8'))


people = ["宝宝","妈","爸","爷","老爹","我爹","奶奶","我奶","舅","外甥","叔","父","母亲","婶","姨","儿子","女儿","女婿","公公","婆","姥","哥","弟","姐","妹","老公","丈夫","妻子","媳妇","朋友","孙子"]



def rule_extract(x):
    # item_id,tmp=x.replace('(','').replace(')','').split(',')
    # user_id,title=tmp.split('\001')
    title=x
    words=[]
    tags=[]
    for index,kv in enumerate(title.split()):
        if '_' not in kv:continue
        if len(kv.split('_'))!=2:continue
        word,tag=kv.split('_')
        if not filter(tag[0]):continue
        if word in people:continue
        words.append(word)
        tags.append(tag)
    neg=''
    degree=''
    emo=''
    find_index=-1
    back_find=-1


    emo_list=[]# 优化  一句话里有几个情感
    tag_index=zip(tags,xrange(len(tags)))
    words_index=zip(tags,xrange(len(words)))
    pairs=[]
    for index,word in enumerate(words):#find  degree neg emo
        if word in neg_set:neg=word
        if word in degree_set:degree=word
        back_find=-1
        if word in emo_set:
            emo=word
            find_index=index
            break
    index=find_index
    tag_back=tag_index[:index][::-1]
    property=[]
    for k,v in tag_back: #东西保湿  gobakck
        if k =='n':
            if neg=='':
                for id,word in enumerate(words[index:]):
                    if word in degree:degree=word
                    if word in neg_set:
                        neg=word
                        if id<len(words):neg=word+words[id+1]#东西保湿不太好
                        break
            property.append(words[v])

    if len(emo)>0:
        if len(property)>0:
            pairs.append([' '.join(property[::-1]),degree,neg,emo])
        else:
            pairs.append([u'商品',degree,neg,emo])
    back_find=1
    property=[]
    if back_find==-1:#不错的宝贝
        if len(emo)>0:
            for k,v in tag_index[index:]:
                if k =='n':
                    pairs.append([words[v],degree,neg,emo])
                    break
            if back_find==-1:
                pairs.append([u'商品',degree,neg,emo])
    return [ " ".join(i) for i in pairs]



# print u'保湿' in emo_set
# print [u'保湿']
# print emo_set
# print rule(u'东西_n 保湿_nz')[0]
# print rule(u'挺_d 保湿_nz 的_ude1')[0]

# print rule(u'不错_a 的_ude1 宝贝_n')[0]
# print rule(u'便宜_a 的_ude1 价格_n')[0]
# print rule(u'祛痘_vn 效果_n 很_d 一般_ad')[0]
# print rule(u'去_vf  黑头_n 的_ude1 功效_n 一般_ad')[0]
# print rule(u'不_d 保湿_nz ')[0]
# print rule(u'不_d  防水_vn 和_cc  保湿_nz ')[0]


import  re

fuhao = '[,.!~;:?，。！～；：？…— \t]'.decode('utf-8')
def fenju(feed):
    # sentence = re.sub(fuhao,' ',feed)
    # print sentence
    sentence=feed
    ss = sentence.split("_w")
    #ss = re.split('[,.!~;:?，。！～；：？…— \t]', line.strip())
    #sss = []
    result = []
    for ln in ss:
        rs=rule_extract(ln.strip())
        if len(rs)>0:result.append(ln.strip()+"|"+'\t'.join(rs))
    return result



def f(x):

    id1,id2,feed=x.replace('(,','').replace(')','').split('\001')
    rs=fenju(feed)
    return "\t\t".join([id1,id2,'\t'.join(rs)])


rdd=sc.parallelize(sc.textFile('/user/zlj/temp/table_t_zlj_feed_parse_corpus_2015_seg/part-00000').take(10000))

rdd.map(lambda x:f(x)).saveAsTextFile('/user/zlj/temp/parse')



