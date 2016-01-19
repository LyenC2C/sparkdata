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

'''数据格式
impr_0_0_1 记录否定词 正面词 反面词个数，拥于调试, 每个分句进行判断
neg_pos  每个分句打分累加
impr_c  修改后的属性情感词 商品:柔软:正负面:否定词
'''
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

from collections import defaultdict
f_map = defaultdict(set)


# for  i in '数—码数  行-还行 快递-物流'.split():f_map['k_bad'].add(i.strip().decode('utf-8'))
for  i in '真是  是 就是 必须 怎样 帮 哦 哇 仙 岁 说 覺 包 家 米 它 什么 还 放 经 带 做 下 够 想 断 嗯 斤 像 算 嘿 要 让' \
          '寄 也 人 写 哒 5   11 g 问 巴 太 1 片 儿 搞 打 9 O 6 请 歡 哈 利 身 你 匀 连 为 \' 滿  此 2 20 需 75 占 我' \
          '7 M'.split():f_map['v_bad'].add(i.strip().decode('utf-8'))
for  i in '穿 不知道 件 款 以后 后 儿 天 装 处 放  掉 根 \' 易 这'.split():f_map['k_bad'].add(i.strip().decode('utf-8'))

for  i in '评价:晚 商品:哈哈 商品:不好意思 商品:呵呵 商品:买 商品:真是 商品:就是 商品:购买 商品:懒 商品:还是 商品:亲 以后:需要 商品:透 商品:嘿嘿 商品:热情 商品:耐心  ' \
          '商品:斤 商品:唉 差:多 商品:嘻嘻  商品:哈哈哈 效果:怎么样 亲:下手 不知道:是不是 质量:怎么样 商品:这样 时尚:大方 效果:如何  数:小 商品:怎么样 商品:錯  ' \
          ' 商品:抱歉 数:大 商品:温和 商品:忙 商品:想象 商品:个 商品:犹豫 天:冷 商品:想 不知道:起 好评:好 品:那种 商品:说实话 颜色:没 不知道:用  商品:用  商品:极 ' \
          '效果:怎样 商品:伤心 商品:郁闷 商品:累 商品:好贴 质量:还是 商品:仙 里面:还有 亲:犹豫 棒:极 上:好看 商品:件  天气:冷 体重:斤  亲:放心 我:用  商品:滑滑 ' \
          '质量:如何 不知道:是 回头率:高' \
          '商品:不知道 商品:高兴 商品:润 商品:重要 商品:闪 小:多 商品:年 商品:啦啦啦 商品:元 商品:穿 商品:那种 我:在 我:给 商品:有 感:覺 己:经 之前:买 商品:送 亲:下' \
          '商品:财源广进 我:给 亲:买 商品:好孩子 孩子:岁 第一次:是 商品:家 玩:开心 性:强 不知道:长'.split():f_map['kv_bad'].add(i.strip().decode('utf-8'))

#分是五分。 句法分析分析不出来的
for  i in '好  很好 不错  挺好  棒 给力 好看 分 棒棒 一如既往'.split():f_map['good_pos'].add(i.strip().decode('utf-8'))
for  i in '差劲 垃圾 差 烂'.split():f_map['good_neg'].add(i.strip().decode('utf-8'))
for  i in '快  很快  速度  神速 飞快 迅速 块'.split():f_map['wuliu_pos'].add(i.strip().decode('utf-8'))
for  i in '慢慢 慢 蜗牛'.split():f_map['wuliu_neg'].add(i.strip().decode('utf-8'))
for  i in '热情 周到 耐心  解答 回答  讲解  细心 有问必答'.split():f_map['fuwu_pos'].add(i.strip().decode('utf-8'))
for  i in '严实  完好 严密 扎实 完好无损   完整'.split():f_map['baozhuang_pos'].add(i.strip().decode('utf-8'))
for  i in '损坏  破损  碰损  毁损 损毁'.split():f_map['baozhuang_neg'].add(i.strip().decode('utf-8'))
for  i in '实惠 便宜  物超所值 超值 值'.split():f_map['jiage_pos'].add(i.strip().decode('utf-8'))
for  i in '贵'.split():f_map['jiage_neg'].add(i.strip().decode('utf-8'))

#数:正  需要修改
#物美价廉:来:
# 数:合身
# 质量:错:-1:
# filter_words=' '
#
# for i in filter_words.split():f_map['filter'].add(i.strip().decode('utf-8'))



def merge(k,v):
    k1=k
    v1=v
    k=f_coding(k)
    v=f_coding(v)
    if v in f_map['good_pos']:
        v='好'
    if v in f_map['good_neg']:
        v='差'
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
        k='价格'
        v='实惠'
    if v in f_map['jiage_neg']:
        k='价格'
        v='贵'
    if k+":"+v in('物:超','物:值'):
        k='价钱'
        v='实惠'
    if k in ('价款','价钱'): k='价格'
    if  (k1==k)==False or (v1==v)==False:return True,k,v  #发生改变
    else: return False,k,v



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
                neg+=flag
                if ts[-1] in f_map['kv_bad']:# filter  dic 两者并不相同
                    ls.append(i+'_'+scores)
                    continue
                if ":" in i:
                    k,v=ts[-1].split(':')
                    if k in f_map['k_bad']:ls.append(i+'_'+scores); continue
                    if v in f_map['v_bad']:ls.append(i+'_'+scores); continue
                    change,k1,v1=merge(k,v)
                    if change==False and  (not dic.has_key(k1+":"+v1)):
                        ls.append(i+'_'+scores) ; continue #没有改变并且不再字典里面
                    rs.append(f_coding(k1)+":"+f_coding(v1)+":"+str(flag)+":"+neg_word)
                    ts[-1]=k+":"+v
                    ls.append(",".join(ts)+'_'+scores) #改写写回
                else:
                    ls.append(i+'_'+scores)
            if neg>0:neg=1
            elif neg==0:neg=0
            else: neg=-1
            return [item_id,feed_id,user_id,feed,'|'.join(ls),neg,'|'.join(rs)]
        except:return None
    # return feed+'\t'+'|'.join(ls)



neg_line="不是 不太 不能 不可以 没有  木有 没 未 别 莫 勿 不够 不必 甭 不曾 不怎么 不如 无 不是 并未 不太 绝不 谈不上 看不出 达不到 并非 从不 从没 毫不 不肯 有待 无法 没法 毫无 没有什么 没什么"

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


# path='/user/zlj/data/feed_2015_alicut_parse/parse_split_clean_cut_part-00000_0002'
path='/user/zlj/data/feed_2015_alicut_parsev3/*'

filter_path='/user/zlj/data/feed_2015_alicut_parse_rank_1/part-00000'

# test

# rdd=sc.textFile(path).map(lambda x:x.split()).filter(lambda x:len(x)!=5).filter(lambda x:x[1]=='257393629511')

filter_impr_dic=sc.textFile(filter_path).map(lambda x:x.split()).filter(lambda x: int(x[0])>17).map(lambda x:(x[-1],1)).collectAsMap()


filter_impr_dic=sc.broadcast(filter_impr_dic)



# rdd=sc.textFile(path).map(lambda x:getfield(x,filter_impr_dic.value)).filter(lambda x:x is not None).map(lambda x: '\t'.join([ f_coding(i) for i in x]))
# rdd.saveAsTextFile('/user/zlj/data/feed_2015_alicut_parse_emo_test')



hiveContext.sql('use wlbase_dev')
rdd=sc.textFile(path).map(lambda x:getfield(x,filter_impr_dic.value)).filter(lambda x:x is not None)
df=hiveContext.createDataFrame(rdd,schema1)
hiveContext.registerDataFrameAsTable(df,'temp_zlj')
hiveContext.sql('drop table  if EXISTS t_zlj_feed2015_parse_v3')
hiveContext.sql('create table t_zlj_feed2015_parse_v3 as select * from temp_zlj')


