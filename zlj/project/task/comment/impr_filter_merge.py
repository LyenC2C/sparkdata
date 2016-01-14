#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import *
from pyspark.sql import *

conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
sc = SparkContext(appName="impr",conf=conf)
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

'''
错误标签过滤 以及 相似标签合并
'''

'''
卖家服务 :热情 周到 耐心  解答 回答  讲解  细心 有问必答  服务

---服务好

物流   :快  很快  速度  神速

质量:  好  很好 不错  挺好

商品:  好 很好  不错

价格： 棒 好

'''
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

from collections import defaultdict

f_map = defaultdict(set)
# f_map['good'].union([i.strip().decode('utf-8') for  i in '好  很好 不错  挺好  棒'.split()])
# f_map['wuliu'].union([i.strip().decode('utf-8') for  i in '快  很快  速度  神速'.split()])
# f_map['fuwu'].union([i.strip().decode('utf-8') for  i in '热情 周到 耐心  解答 回答  讲解  细心 有问必答  服务'.split()])


for  i in '好  很好 不错  挺好  棒 给力'.split():f_map['good_pos'].add(i.strip().decode('utf-8'))
for  i in '差劲 垃圾 差'.split():f_map['good_neg'].add(i.strip().decode('utf-8'))

for  i in '快  很快  速度  神速'.split():f_map['wuliu_pos'].add(i.strip().decode('utf-8'))
for  i in '慢慢 慢 蜗牛'.split():f_map['wuliu_neg'].add(i.strip().decode('utf-8'))

for  i in '热情 周到 耐心  解答 回答  讲解  细心 有问必答  服务'.split():f_map['fuwu_pos'].add(i.strip().decode('utf-8'))

for  i in '严实  完好 严密 扎实 完好无损   完整'.split():f_map['baozhuang_pos'].add(i.strip().decode('utf-8'))
for  i in '损坏  破损  碰损  毁损 损毁'.split():f_map['baozhuang_neg'].add(i.strip().decode('utf-8'))


for  i in '实惠 便宜  物超所值 超值'.split():f_map['jiage_pos'].add(i.strip().decode('utf-8'))


def merge(k,v):
    k=f_coding(k)
    v=f_coding(v)
    if v in f_map['good_pos']:
        v='好'
    if v in f_map['good_neg']:
        v='好'
    if (v in f_map['wuliu_pos']) or (k=='快递' and v=='好'):#如果是物流的数据 直接返回物流
        k='物流'
        v='快'
    if v in f_map['wuliu_neg']:#如果是物流的数据 直接返回物流
        k='物流'
        v='慢'
    if v in  f_map['fuwu_pos'] or k in f_map['fuwu_pos']:
        k='服务'
        v='好'
    if v in f_map['baozhuang_pos']:
        k='包装',
        v='完好'
    if v in f_map['baozhuang_neg']:
        k='包装',
        v='差'
    if v in f_map['jiage_pos']:
        k='价钱'
        v='实惠'
    if k+":"+v in('物:超','物:值'):
        k='价钱'
        v='实惠'
    if k in ('价款','价格'): k='价钱'
    return k,v

def getfield(x,dic):
    lv=x.split()
    rs=[]
    ls=[]
    if len(lv)!=5: return None
    else:
        try:
            item_id,feed_id,user_id,feed,impr=lv
            neg=0 #默认中评
            for i in impr.split('|'):
                ts=i.split(',')
                flag,scores,neg_word=pos_neg(ts[0])
                ls.append(i+'_'+scores)
                neg+=flag
                if ":" in i:
                    k,v=ts[-1].split(':')
                    if feed_id=='257393629511':
                        print impr.split('|')
                        print f_map['wuliu_pos']
                    k1,v1=merge(k,v)
                    # if not dic.has_key(k1+":"+v1):continue
                    rs.append(f_coding(k1)+":"+f_coding(v1)+":"+str(flag)+":"+neg_word)
            return [item_id,feed_id,user_id,feed,'|'.join(ls),str(neg),'|'.join(rs)]
        except:return None
    # return feed+'\t'+'|'.join(ls)


neg_line="不是 不太 不能 不可以 没有 沒有 木有 没 未 别 莫 勿 不够 不必 甭 不曾 不怎么 不如 无 不是 并未 不太 绝不 谈不上 看不出 达不到 并非 从不 从没 毫不 不肯 有待 无法 没法 毫无 没有什么 没什么"

neg_path='/user/zlj/data/neg'
pos_path='/user/zlj/data/pos'
neg_list=sc.textFile(neg_path).map(lambda x:x.strip()).collect()
neg_set=sc.broadcast(neg_list)

pos_list=sc.textFile(pos_path).map(lambda x:x.strip()).collect()
pos_set=sc.broadcast(pos_list)

pos_emo_set=set(pos_set.value)
neg_emo_set=set(neg_set.value)
neg_set    =set([i.strip().decode('utf-8') for  i in neg_line.split()])
def pos_neg(words):
    words_set=set(words.split('\002'))
    neg = len(neg_set&words_set)
    neg_emo = len(neg_emo_set&words_set)
    pos_emo = len(pos_emo_set&words_set)
    flag=0
    if neg>0 and pos_emo>0:flag= -1
    if neg>0 and neg_emo>0: flag= 1
    if neg==0 and pos_emo>0: flag= 1
    if neg==0 and neg_emo>0: flag= -1
    if neg==0 and pos_emo==0 and neg_emo==0 :flag= 0
    neg_word=""
    if neg>0:neg_word=(neg_emo_set&words_set)[0]
    return flag,'_'.join(str(i) for i in [neg,neg_emo,pos_emo]),neg_word


path='/user/zlj/data/feed_2015_alicut_parse/parse_split_clean_cut_part-00000_0002'
# path='/user/zlj/data/feed_2015_alicut_parse/*'

filter_path='/user/zlj/data/feed_2015_alicut_parse_rank_1/part-00000'

# test

# rdd=sc.textFile(path).map(lambda x:x.split()).filter(lambda x:len(x)!=5).filter(lambda x:x[1]=='257393629511')

filter_impr_dic=sc.textFile(filter_path).map(lambda x:x.split()).filter(lambda x: int(x[0])>17).map(lambda x:(x[-1],1)).collectAsMap()

filter_impr_dic=sc.broadcast(filter_impr_dic)

rdd=sc.textFile(path).map(lambda x:getfield(x,filter_impr_dic.value)).filter(lambda x:x is not None).map(lambda x: '\t'.join([ f_coding(i) for i in x]))
rdd.saveAsTextFile('/user/zlj/data/feed_2015_alicut_parse_emo_test')




'''数据格式
impr_0_0_1 记录否定词 正面词 反面词个数，拥于调试, 每个分句进行判断
neg_pos  每个分句打分累加
impr_c  修改后的属性情感词 商品:柔软:正负面:否定词
'''
# schema1 = StructType([
#     StructField("item_id", StringType(), True),
#     StructField("feed_id", StringType(), True),
#     StructField("user_id", StringType(), True),
#     StructField("feed", StringType(), True),
#     StructField("impr", StringType(), True),
#     StructField("neg_pos", StringType(), True),
#     StructField("impr_c", StringType(), True)
#     ])
#
#
# hiveContext.sql('use wlbase_dev')
# rdd=sc.textFile(path).map(lambda x:getfield(x,filter_impr_dic.value)).filter(lambda x:x is not None)
# df=hiveContext.createDataFrame(rdd,schema1)
# hiveContext.registerDataFrameAsTable(df,'temp_zlj')
# hiveContext.sql('drop table  if EXISTS t_zlj_feed2015_parse_v2')
# hiveContext.sql('create table t_zlj_feed2015_parse_v2 as select * from temp_zlj')



# sc.textFile('/user/zlj/data/feed_2015_alicut_parse_emo_test').map(lambda x:x.split()[-2]).histogram(3)