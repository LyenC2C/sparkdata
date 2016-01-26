#coding:utf-8
__author__ = 'zlj'
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *


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

tag_f=set(u'mbceopquyrw')


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



emo_set=None

# emo_set=set()
# for line in open('D:\\emo_all'):
#     emo_set.add(line.strip().decode('utf-8'))
# emo_set=emo_set-neg_set-degree_set

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
        if k[0] =='n':
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
            back_find=1
        # else:
        #     pairs.append([u'商品',degree,neg,emo]) # 前面没哟属性词，再看后面 ，在最后处理只有情感词的情况

    property=[]
    find=-1
    if back_find==-1:#不错的宝贝
        if len(emo)>0:
            for k,v in tag_index[index:]:
                if k[0] =='n':
                    if emo!=words[v]: #好评=好评
                        pairs.append([words[v],degree,neg,emo])
                        find=1
                        break
            if find==-1:
                pairs.append([u'商品',degree,neg,emo])
    return [ ":".join(i) for i in pairs]



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
fuhao_seg = '_x_w'.decode('utf-8')
def fenju(feed):
    sentence = feed.split('_w')
    # print sentence
    # sentence=feed
    ss = []
    for line in sentence:
        for k in line.split('_x'):
            ss.append(k)
    #ss = re.split('[,.!~;:?，。！～；：？…— \t]', line.strip())
    #sss = []
    result = []
    for ln in ss:
        rs=rule_extract(ln.strip())
        if len(rs)>0:result.append("".join(ln.strip())+"|"+'\t'.join(rs))
        else:result.append("".join(ln.strip()))
    return result



def f(x):
    id1,id2,feed=x.replace('(,','').replace(')','').split('\001')
    rs=fenju(feed)
    return "\t\t".join([id1,id2,'\t'.join(rs)])



feed=u'很_d 牢固_a'
print '\t'.join(fenju(feed))

conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
sc = SparkContext(appName="impr",conf=conf)
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

emo_rdd=sc.textFile('/user/zlj/data/emo_all').collect()
neg_set_bc=sc.broadcast(emo_rdd)
emo_set=set(neg_set_bc.value)
emo_set=emo_set-neg_set-degree_set

rdd=sc.parallelize(sc.textFile('/user/zlj/temp/table_t_zlj_feed_parse_corpus_2015_seg/part-00000').take(10000))

rdd.map(lambda x:f(x)).saveAsTextFile('/user/zlj/temp/parse')




# rdd=sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_feed2015_parse_v3/').map(lambda x:x.split('\001')[-1])\
#     .filter(lambda x:len(x)>2).map(lambda x:x.split('|')).flatMap(lambda x:x).map(lambda x:(x.split(':')[1],1))
#
# rdd1=rdd.reduceByKey(lambda a,b:a+b).repartition(1).map(lambda x:(x[1],x[0])).sortByKey(ascending=False)
#
# rdd1.map(lambda x:str(x[0])+'\t'+x[1]).saveAsTextFile('/user/zlj/temp/emo_count')