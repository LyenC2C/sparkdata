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
'''
错误标签过滤 以及 相似标签合并
'''

'''数据格式
impr_0_0_1 记录否定词 正面词 反面词个数，拥于调试, 每个分句进行判断
neg_pos  每个分句打分累加
impr_c  修改后的属性情感词 商品:柔软:正负面:否定词
'''
schema1 = StructType([
    StructField("item_id", StringType(), True),
    StructField("user_id", StringType(), True),
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

# 分---.5分
# for  i in '数—码数  行-还行 快递-物流'.split():f_map['k_bad'].add(i.strip().decode('utf-8'))
for  i in '真是  嘎嘎 嘎 嘎嘎嘎 哈 哈哈 哈哈哈 呵呵  呵 唉 是 就是 必须 怎样 帮 哦 哇 仙 岁 说 覺 包 家 米 它 什么 还 放 经 带 做 下 够 想 断 嗯 斤 像 算 嘿 要 让' \
          '寄 也 人 写 哒 5   11 g 问 巴 太 1 片 儿 搞 打 9 O 6 请 歡 哈 利 身 你 匀 连 为 \' 滿  此 2 20 需 75 占 我' \
          '7 M'.split():f_map['v_bad'].add(i.strip().decode('utf-8'))
for  i in '穿 不知道 件 款 以后 后 儿 天 装 处 放  掉 根 \' 易 这'.split():f_map['k_bad'].add(i.strip().decode('utf-8'))

bad_kv_path='/user/zlj/data/bad_kv_0120'
bad_kv_list=sc.textFile(bad_kv_path).map(lambda x:x.strip()).collect()
bad_kv_set=sc.broadcast(bad_kv_list)
# for i in bad_kv_set.value:f_map['kv_bad'].add(i.strip())


#分是五分。 句法分析分析不出来的
for  i in '好  很好 不错  挺好  棒 给力 好看 分 棒棒 一如既往'.split():f_map['good_pos'].add(i.strip().decode('utf-8'))
for  i in '差劲 垃圾 差 烂 孬'.split():f_map['good_neg'].add(i.strip().decode('utf-8'))
for  i in '快  很快  速度  神速 飞快 迅速 块'.split():f_map['wuliu_pos'].add(i.strip().decode('utf-8'))
for  i in '慢慢 慢 蜗牛'.split():f_map['wuliu_neg'].add(i.strip().decode('utf-8'))
for  i in '贴心 热情 周到 耐心  解答 回答  讲解  细心 有问必答' \
          '热心 热忱 热心肠 好客 满腔热忱 古道热肠 有求必应 来者不拒 急人之难 急人所急 满腔热情 满怀深情 热情洋溢'.split():f_map['fuwu_pos'].add(i.strip().decode('utf-8'))
for  i in '严实  完好 严密 扎实 完好无损   完整'.split():f_map['baozhuang_pos'].add(i.strip().decode('utf-8'))
for  i in '损坏  破损  碰损  毁损 损毁'.split():f_map['baozhuang_neg'].add(i.strip().decode('utf-8'))
for  i in '实惠 便宜  物超所值 超值 值 物美价廉 低廉 廉价 公道 惠而不费 价廉物美 物美价廉 最低价 价廉 质优价廉 价廉质优 低价'.split():f_map['jiage_pos'].add(i.strip().decode('utf-8'))
for  i in '贵'.split():f_map['jiage_neg'].add(i.strip().decode('utf-8'))

#数:正  需要修改
#物美价廉:来:
# 数:合身
# 质量:错:-1:
# filter_words=' '
#
# for i in filter_words.split():f_map['filter'].add(i.strip().decode('utf-8'))

mapline=u'数_码数 分_5分 行_还行 错_不错 价款_价格 价钱_价格'
modify_map={}
for kv  in mapline.split():
    k,v =kv.split('_')
    modify_map[k]=v

def merge(k,v):
    k1=k
    v1=v
    k=f_coding(k)
    v=f_coding(v)
    if modify_map.has_key(v):
        v=modify_map.get(v)
    if modify_map.has_key(k):
        k=modify_map.get(k)
    if v in f_map['good_pos']:
        v='好'
    if v in f_map['good_neg']:
        v='差'
    if (v in f_map['wuliu_pos']) or(k in f_map['wuliu_pos']) or (k=='快递' and v=='好'):#如果是物流的数据 直接返回物流
        k='物流'
        v='快'
    if v in f_map['wuliu_neg']:#如果是物流的数据 直接返回物流
        k='物流'
        v='慢'
    if v in  f_map['fuwu_pos'] or k in f_map['fuwu_pos']:
        k='服务'
        v='好'
    if v in f_map['baozhuang_pos']:
        k='包装'
        v='完好'
    if v in f_map['baozhuang_neg']:
        k='包装'
        v='差'
    if v in f_map['jiage_pos']:
        k='价格'
        v='实惠'
    if v in f_map['jiage_neg']:
        k='价格'
        v='贵'
    # print type(k),type(v),k,v
    if f_coding(k)+u":"+f_coding(v) in (u'物:超',u'物:值'):
        k='价格'
        v='实惠'
    if  (k1==k)==False or (v1==v)==False:return True,k,v  #发生改变
    else: return False,k,v


# 规则重新整理标签
'''
有 情感，往前找否定词 和名字 否定+情感   d+情感

或者情感往后缀   情感词-否定词

'''
degree_line=u'''更_2
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
倍儿_2
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
有一点点_1
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
degree_map={}
for kv in degree_line.split():
    if len(kv)<1:continue
    k,v=kv.split('_')
    degree_map[k]=int(v)

degree_set=set()

for kv in degree_line.split():
    lv=kv.strip().split('_')
    if len(lv)!=2:continue
    k,v =lv
    degree_set.add(k)



neg_line="不是 不太 不能 不可以 没有 不 不行 木有 没  未 别 莫 勿 不够 不必 甭 不曾 不怎么 不如 无 不是 并未 不太 绝不 谈不上 看不出 达不到 并非 从不 从没 毫不 不肯 有待 无法 没法 毫无 没有什么 没什么"

neg_path='/user/zlj/data/neg'
pos_path='/user/zlj/data/pos'
neg_list=sc.textFile(neg_path).map(lambda x:x.strip()).collect()
neg_add='不靠谱 '
for word in neg_add.split():
    neg_list.append(word.strip().decode('utf-8'))
neg_set_bc=sc.broadcast(neg_list)

# 极
pos_add='快 很快 好吃 高 实惠 可以 超高'
pos_list=sc.textFile(pos_path).map(lambda x:x.strip()).collect()
for word in pos_add.split():
    pos_list.append(word.strip().decode('utf-8'))
pos_set_bc=sc.broadcast(pos_list)

pos_emo_set=set(pos_set_bc.value)
neg_emo_set=set(neg_set_bc.value)

neg_set    =set([i.strip().decode('utf-8') for  i in neg_line.split()])



emo_set=set()
emo_rdd=sc.textFile('/user/zlj/data/emo_all').collect()
neg_set_bc=sc.broadcast(emo_rdd)
emo_set=set(neg_set_bc.value)
emo_set=emo_set-neg_set-degree_set

people = ["朋友","宝宝","妈","爸","爷","老爹","我爹","奶奶","我奶","舅","外甥","叔","父","母亲","婶","姨","儿子","女儿","女婿","公公","婆","姥","哥","弟","姐","妹","老公","丈夫","妻子","媳妇","朋友","孙子"]


tag_f=set(u'mbceopquyrw')
def filter_t(tag):
    if tag in tag_f:
        return False
    else: return True

def rule_extract(x):
    # item_id,tmp=x.replace('(','').replace(')','').split(',')
    # user_id,title=tmp.split('\001')
    title=x
    words=[]
    tags=[]
    for index,kv in enumerate(title.split('\002')):
        if '_' not in kv:continue
        if len(kv.split('_'))!=2:continue
        word,tag=kv.split('_')
        if len(tag)<1:continue
        if not filter_t(tag[0]):continue
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
    return "@@".join([ i[0]+":"+i[3]+"_"+i[2]+"_"+i[1] for i in pairs])


# 有词性
def getfield(x,dic):
    lv=x.split()
    rs=[]
    ls=[]
    if len(lv)!=3: return None
    else:
        try:
            item_id,user_id,impr=lv
            neg=0 #默认中评
            for i in impr.split('|'):
                ts=i.split(',')
                flag,scores,neg_word=pos_neg(ts[0])
                neg+=flag
                if ts[-1].split('_')[0] in f_map['kv_bad']:# filter  dic 两者并不相同
                    ls.append(i+'_'+scores)
                    continue
                if ":" not  in ts[-1]:
                    find_kv=rule_extract(ts[0])
                    if len(find_kv)>0:
                        ts.append(find_kv)#重新加入rule捕获的tag
                if ":" in ts[-1]:
                    try:
                        k,v=ts[-1].split('_')[0].split(':')
                    except:
                        # print f_coding(i)
                        continue
                    if k in f_map['k_bad']:ls.append(i+'_'+scores); continue
                    if v in f_map['v_bad']:ls.append(i+'_'+scores); continue
                    change,k1,v1=merge(k,v)
                    if change==False and  (f_coding(v1) not in emo_set) or (not dic.has_key(f_coding(k1+":"+v1))):
                        ls.append(i+'_'+scores) ; continue #没有改变并且不再字典里面
                    # print [f_coding(k1),f_coding(v1),str(flag),neg_word]
                    rs.append(f_coding(k1)+":"+f_coding(v1)+":"+str(flag)+":"+neg_word)
                    ts[-1]=k+":"+v
                    ls.append(",".join(ts)+'_'+scores) #改写写回
                else:
                    ls.append(i+'_'+scores)
            if neg>0:neg=1
            elif neg==0:neg=0
            else: neg=-1

            if(len(ls)<1):ls.append(i)
            return [item_id,user_id,'|'.join(ls),neg,'|'.join(rs)]
        except:return None
    # return feed+'\t'+'|'.join(ls)




def pos_neg(words):

    words_set=set([i.split('_')[0] for i in words.split('\002')]) #鱼轮_n很_d好_a
    lv=[]
    for word in words_set: #加入程度
        if degree_map.has_key(word):lv.append(degree_map.get(word))
    degree=0 if len(lv)==0 else max(lv)
    neg = len(neg_set&words_set)
    if neg%2==0: neg=0
    else :neg=1
    neg_emo = len(neg_emo_set&words_set)
    pos_emo = len(pos_emo_set&words_set)
    flag=0
    if neg>0 and pos_emo>0:flag= -1
    if neg>0 and neg_emo>0: flag= 1
    if neg==0 and pos_emo>0: flag= 1
    if neg==0 and neg_emo>0: flag= -1
    if neg==0 and pos_emo==0 and neg_emo==0 :flag= 0
    neg_word=""
    if neg>0:
        try:
            neg_word=list((neg_emo_set&words_set))[0]
        except:pass
    return flag,'_'.join(str(i) for i in [neg*degree,neg_emo,pos_emo]),neg_word


# path='/user/zlj/data/feed_2015_alicut_parse/parse_split_clean_cut_part-00000_0002'
# path='/user/zlj/data/feed_2015_alicut_parsev3/*'

# path='/user/zlj/data/feed_2015_alicut_parsev4/parse_cut_part-00000'
# path='/user/zlj/data/feed_2015_alicut_parsev4/*'
path='/user/zlj/data/1'

filter_path='/user/zlj/data/feed_2015_alicut_parse_rank_1/part-00000'

# test

# rdd=sc.textFile(path).map(lambda x:x.split()).filter(lambda x:len(x)!=5).filter(lambda x:x[1]=='257393629511')

filter_impr_dic=sc.textFile(filter_path).map(lambda x:x.split()).filter(lambda x: int(x[0])>17).map(lambda x:(x[-1],1)).collectAsMap()


filter_impr_dic=sc.broadcast(filter_impr_dic)


# rdd=sc.textFile(path).map(lambda x:getfield(x,filter_impr_dic.value)).filter(lambda x:x is not None).map(lambda x: '\t'.join([ f_coding(i) for i in x]))
# rdd.saveAsTextFile('/user/zlj/data/feed_2015_alicut_parse_emo_test')

hiveContext.sql('use wlbase_dev')


# def try_getfield(x,dic):
#     try:
#         return getfield(x,dic)
#     except:
#         pass
rdd=sc.textFile(path).map(lambda x:getfield(x,filter_impr_dic.value)).filter(lambda x:x is not None)
# rdd.fold()
df=hiveContext.createDataFrame(rdd,schema1)
hiveContext.registerDataFrameAsTable(df,'temp_zlj')
hiveContext.sql('drop table  if EXISTS t_zlj_feed2015_parse_v4_1')
hiveContext.sql('create table t_zlj_feed2015_parse_v4_1 as select * from temp_zlj')





# sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_feed2015_parse_v4/').map(lambda x:x.split('\001')[-1].split('|')).\
#     flatMap(lambda x:x).filter(lambda x:len(x)>0).count().filter(lambda x:len(x)>0).count()

# sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_feed2015_parse_v4').map(lambda x:x.split('\001')[-1].split('|')).flatMap(lambda x:x).count()


