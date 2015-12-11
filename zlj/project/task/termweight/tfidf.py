#coding:utf-8
__author__ = 'zlj'
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark import SparkContext

from pyspark.sql import *
from pyspark.sql.types import *
from itertools import groupby

sc = SparkContext(appName="term weight")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

hiveContext.sql('use wlbase_dev')

'''

'''
def join(x, y):
    word = x
    tfinfo = y[1]
    idfinfo = y[0]
    doc_id = tfinfo[0]
    tf = tfinfo[1]
    idf = idfinfo
    return (doc_id, (word, tf * idf))


# ['apple', 'banana', 'apple', 'strawberry', 'banana', 'lemon']
# [('apple', 2), ('banana', 2), ('lemon', 1), ('strawberry', 1)]
# return (word,(docid,tf))
def tf(x, y):
    num = len(y)
    lv = [(k, len(list(g)) * 1.0 / num) for k, g in groupby(sorted(y))]
    return [(i[0], (x, i[1])) for i in lv]


def df(x, y):
    lv = []
    s=set(y)
    for i in s:
        lv.append((i, 1))
    return lv

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("tfidftags", StringType(), True)
])

# sql = '''
# select
# * from t_zlj_userbuy_item_hmm
# '''

# min_freq = 100
# limit = 5
# schema = StructType([
#     StructField("user_id", StringType(), True),
#     StructField("tfidftags", StringType(), True)
# ])

def join1(x,dict,worddic):
    word_index=x[0]
    doc_id=x[1][0]
    tf=x[1][1]
    tfidf=tf*dict.get(word_index,0.5)
    word=word_index
    if(word.endswith('-b')):
        tfidf=tfidf*1.3
        # word=word.replace('-b','')
    elif(word.endswith('-c')):
        tfidf=tfidf*1.2
        # word=word.replace('-c','')
    elif(word.endswith('_B1')):
        tfidf=tfidf*1.3
        # word=word.replace('_B1','')
    elif(word.endswith('_B2')):
        tfidf=tfidf*1.2
        # word=word.replace('_B2','')
    elif(word.endswith('_E1')):
        tfidf=tfidf*1.5
        # word=word.replace('_E1','')
    elif(word.endswith('_E2')):
        tfidf=tfidf*1.3
        # word=word.replace('_E2','')
    word=worddic.get(word_index.split('_')[0],'')
    tfidf=tfidf*math.log(len(word)/2.0+2,2)
    return (doc_id,(word,tfidf))

def join1ali(x,idfdict,worddic):
    word_index=x[0]
    doc_id=x[1][0]
    tf    =x[1][1]
    ff=word_index/1200000
    tfidf=tf*idfdict.get(word_index,0.5)
    if(ff==5):
        tfidf=tfidf*1.3
        # word=word.replace('-b','')
    elif(ff==6):
        tfidf=tfidf*1.2
        # word=word.replace('-c','')
    elif(ff==2):
        tfidf=tfidf*1.3
        # word=word.replace('_B1','')
    elif(ff==7):
        tfidf=tfidf*1.2
        # word=word.replace('_B2','')
    elif(ff==4):
        tfidf=tfidf*1.5
        # word=word.replace('_E1','')
    elif(ff==3):
        tfidf=tfidf*1.3
        # word=word.replace('_E2','')
    word=worddic.get(word_index%1200000,'')
    tfidf=tfidf*math.log(len(word)/2.0+3,3)
    return (doc_id,(word_index,tfidf))

sql_hmm='''
select
/*+ mapjoin(t1)*/
user_id, concat_ws(' ', collect_set(hmm)) as hmm

from
(
select item_id,title_cut as hmm from t_zlj_corpus_item_seg
where LENGTH (title_cut)>3
)t1
join
(
select item_id,user_id from t_base_ec_item_feed_dev

where ds>%s
and  LENGTH (user_id)>0

)t2
on t1.item_id=t2.item_id
group by user_id
'''

# select user_id as item_id,title_cut  as hmm from t_base_ec_item_title_cut t_zlj_corpus_item_seg_tfidf
sql_tfidf='''
select
/*+ mapjoin(t1)*/
user_id, concat_ws(' ', collect_set(hmm)) as hmm

from
(
select user_id as item_id,title_cut  as hmm from t_base_ec_item_title_cut
)t1
join
(
select item_id,user_id from t_base_ec_item_feed_dev

where ds>%s
and  LENGTH (user_id)>0

)t2
on t1.item_id=t2.item_id
group by user_id
'''


sql_tfidfbrand='''
select

user_id, concat_ws('@_@', collect_set(hmm)) as hmm

from
(
    select item_id,concat_ws(' ',title_cut_stag,concat(cat_name,'-c_n'), concat(brand_name,'-b_n'))  as hmm
     from t_base_ec_item_title_cut_with_brand_tag_c
     where LENGTH(item_id)>0
)t1
join
(
select item_id,user_id from t_base_ec_item_feed_dev
where ds>%s
and  LENGTH (user_id)>0 and  LENGTH (item_id)>0

group by item_id,user_id

)t2
on t1.item_id=t2.item_id
group by user_id
'''
# testtable t_base_ec_item_feed_dev_temp

'''
rdd  {k,[w1 w2 w3 .....]}

return rdd [id, word_tfifd word_tfidf]
'''

def tcount(lv):
    s=set(lv)
    re=[]
    for i in s:
        re.append((i,lv.count(i)))
    return re


# 最后合并
# [ (word,tfidf) .....]
def groupvalue(y,worddic):
    s1={}
    s2={}
    for k,v in y:
        k=worddic.get(k%1200000,'')
        if len(k)<1: continue
        if k.endswith('-b') or k.endswith('-c'):
            s1[k]=s1.get(k,0)+v
        else :
            s2[k]=s1.get(k,0)+v
    for k,v in s2.iteritems():
        flag=0
        for k1,v1 in s1.iteritems():
            if k in k1:
                s1[k1]=s1[k1]+s2[k] #merge
                flag=1
                break
        if flag==0:s1[k]=s2[k]

    lv=[(k.split('-')[0],v) for k,v in s1.iteritems()]
    # for key, group in itertools.groupby(y, lambda item: item[0]):
    #     lv.append((key, sum([item[1] for item in group])))
    return lv
def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content
def f_coding(x):
    if type(x) == type(""):
        return x.decode("utf-8")
    else:
        return x

import math
'''
支持大规模数据集
需要自定义word词表
'''

def tfidf(rdd_pre,top_freq,min_freq,limit,index_file):
    # top_freq=int(124706*2.5)
    # min_freq=5
    wordrdd=sc.textFile(index_file).map(lambda x:x.split())\
        .filter(lambda x:int(x[2])<top_freq and int(x[2])>min_freq  and (x[1].find('-c')<0))
    words=wordrdd.map(lambda  x:(x[0],x[1])).collectAsMap()
    # word_size=sc.textFile(index_file).count()
    # s=rdd_pre.map(lambda (x,y):[ i for i in y if i.find('_')]).flatMap(lambda x:x).distinct()
    # pos_index={}
    # for index,i in enumerate(s,word_size):
    #     pos_index[i]=index

    # words=wordrdd.map(lambda  x:(int(x[0]))).collect()
    broadcastVar = sc.broadcast(words)
    worddic = broadcastVar.value
    # doc_num = hiveContext.sql('select user_id from t_base_ec_item_feed_dev_temp group by user_id').count()
    doc_num = 50000000
    # {}.get()
    # {}.has_key()
    # rdd = rdd_pre.map(lambda (x, y): (x, [worddic.get(i) for i in y if worddic.has_key(i)]))
    rdd = rdd_pre.map(lambda (x, y): (x, [i for i in y if    worddic.has_key(i.split('_')[0])]))
    # (word,(doc_id,tf))
    tfrdd = rdd.map(lambda (x, y): tf(x, y)).flatMap(lambda x: x)
    # word ,len
    # dfrdd = rdd.map(lambda (x, y): df(x, y)).flatMap(lambda x: x).groupByKey().map(lambda (x, y): (x, len(y)))
    dfrdd = rdd.map(lambda (x, y): df(x, y)).flatMap(lambda x: x).reduceByKey(lambda a,b:a+b)
    # word idf
    idfrdd = dfrdd.map(lambda (x, y): (x, math.log((doc_num + 1) * 1.0 / (y + 1))))
    # idfrdd.collectAsMap()
    # idfrdd.join()
    broadcastVar = sc.broadcast(idfrdd.collectAsMap())
    idfdict = broadcastVar.value
    # rddjoin=idfrdd.join(tfrdd)
    # worddicRs = dict(zip(worddic.values(),worddic.keys()))
    joinrs=tfrdd.map(lambda  x: join1(x,idfdict,worddic))
    # rddjoin = tfrdd.join(idfrdd)
    # sorted(a,key=a[1],reverse=True)
    # rst=rddjoin.map(lambda (x, y): join(x, y))
    jrdd=joinrs.groupByKey()
    jrdd.map(lambda (x,y):str(x)+'\001'.join([str(k)+":"+str(v) for k,v in y ])).saveAsTextFile('/user/zlj/project/termweight/jointfidf_rs')
    rst=jrdd.map(lambda (x, y):(x,groupvalue(y))).map(lambda (x,y):[x, "\t".join(
        [i[0]+"_"+str(i[1]) for index, i in enumerate(sorted(y, key=lambda t: t[-1], reverse=True)) if index < limit])])
    return rst


def tfidfali(rdd_pre,top_freq,min_freq,limit,index_file):
    # top_freq=int(124706*2.5)
    # min_freq=5
    wordrdd=sc.textFile(index_file).map(lambda x:x.split())\
        .filter(lambda x:int(x[2])<top_freq and int(x[2])>min_freq  and (x[1].find('-c')<0))
    words=wordrdd.map(lambda  x:(int(x[0]),x[1])).collectAsMap()

    # words=wordrdd.map(lambda  x:(int(x[0]))).collect()
    broadcastVar = sc.broadcast(words)
    worddic = broadcastVar.value
    # doc_num = hiveContext.sql('select user_id from t_base_ec_item_feed_dev_temp group by user_id').count()
    doc_num = 50000000
    def clean(i):
        ls=i.split('_')
        if len(ls)>1:
            if  'B1' in ls[1]: return 1200000*2+int(ls[0])
            if  'E2' in ls[1]: return 1200000*3+int(ls[0])
            if  'E1' in ls[1]: return 1200000*4+int(ls[0])
        else :return int(i)
    rdd = rdd_pre.map(lambda (x, y): (int(x), [clean(i) for i in y if    worddic.has_key(int(i.split('_')[0]))]))
    # rdd.saveAsTextFile('/user/zlj/temp/rdd_pre')
    # (word,(doc_id,tf))
    tfrdd = rdd.map(lambda (x, y): tf(x, y)).flatMap(lambda x: x)
    # word ,len
    # dfrdd = rdd.map(lambda (x, y): df(x, y)).flatMap(lambda x: x).groupByKey().map(lambda (x, y): (x, len(y)))
    dfrdd = rdd.map(lambda (x, y): df(x, y)).flatMap(lambda x: x).reduceByKey(lambda a,b:a+b)
    # word idf
    idfrdd = dfrdd.map(lambda (x, y): (x, math.log((doc_num + 1) * 1.0 / (y + 1))))
    # idfrdd.collectAsMap()
    # idfrdd.join()
    broadcastVar = sc.broadcast(idfrdd.collectAsMap())
    idfdict = broadcastVar.value
    # rddjoin=idfrdd.join(tfrdd)
    # worddicRs = dict(zip(worddic.values(),worddic.keys()))
    joinrs=tfrdd.map(lambda  x: join1ali(x,idfdict,worddic))
    # rddjoin = tfrdd.join(idfrdd)
    # sorted(a,key=a[1],reverse=True)
    # rst=rddjoin.map(lambda (x, y): join(x, y))
    jrdd=joinrs.groupByKey()

    # jrdd.map(lambda (x,y):(x," ".join([str(k)+":"+str(v) for k,v in y]))).saveAsTextFile('/user/zlj/temp/joinss')
    # jrdd.map(lambda (x,y):str(x)+'\001'.join([str(k)+":"+str(v) for k,v in y ])).saveAsTextFile('/user/zlj/project/termweight/jointfidf_rs')
    rst=jrdd.map(lambda (x, y):(x,groupvalue(y,worddic))).map(lambda (x,y):[str(x), "\t".join(
        [i[0]+"_"+str(i[1]) for index, i in enumerate(sorted(y, key=lambda t: t[-1], reverse=True)) if index < limit])])
    return rst
'''
spark-submit  --total-executor-cores  200   --executor-memory  20g  --driver-memory 20g  tfidf.py -item 200   5  20143 t_zlj_corpus_item_seg item_id title_cut  t_zlj_corpus_item_seg_tfidf
'''

# word_n word_n  word-c_n
# add position
def title_clean(x):
    '''
    文本内容清理
    :param x:文本内容 title内部用002 title之间用003
                    word1\002word2\002\word3\003word1\002word2\002\word3
    :return: 清理后内容，增加了位置信息
    '''
    lv=f_coding(x).split('\003')
    rs=[]
    for i in lv:
        kv=i.split('\002')
        s=len(kv)
        for index,v in enumerate(kv,1):
            if  not v.find('_n'):continue
            word=v.split('_')[0]
            if  len(word)<2:continue
            if word.replace('.','',1).isdigit(): continue
            if index==(s-3):
                rs.append(word+'_E2')
            elif index==(s-2):
                rs.append(word+'_E1')
            elif index==1:
                rs.append(word+'_B1')
            else:
                rs.append(word)
    return rs

def title_clean_ali(x):
    lv=f_coding(x).split('\003')
    rs=[]
    for i in lv:
        kv=i.split('\002')
        s=len(kv)
        for index,v in enumerate(kv,1):
            word=v.split('_')[0]
            if  not word.isdigit():continue
            if index==(s-3):
                rs.append(word+'_E2')
            elif index==(s-2):
                rs.append(word+'_E1')
            elif index==1:
                rs.append(word+'_B1')
            else:
                rs.append(word)
    return rs


import sys
if __name__ == "__main__":

    if len(sys.argv)<4:
        print ' py -usertfidf  min_freq limit feed_ds outputtable'
        print ' py -item min_freq limit feed_ds input_table input_docid, input_talbe_title  output_table'
        sys.exit(0)
    if sys.argv[1]=='-usertfidf':
        i=1
        min_freq=int(sys.argv[i+1])
        limit=int(sys.argv[i+2])
        feed_ds=sys.argv[i+3]
        output_talbe=sys.argv[i+4]
        rdd_pre = hiveContext.sql(sql_tfidf%feed_ds).map(lambda x: (x.user_id, [i.split('_')[0] for i in x[1].split() ]))
        rst=tfidf(rdd_pre,top_freq=1000,min_freq=min_freq,limit=limit)
        df=hiveContext.createDataFrame(rst,schema)
        hiveContext.registerDataFrameAsTable(df, 'tmptable')
        # hiveContext.sql('drop table if EXISTS  t_zlj_userbuy_item_tfidf_tags')
        # hiveContext.sql('create table t_zlj_userbuy_item_tfidf_tags as select * from tmptable')
        hiveContext.sql('drop table if EXISTS  %s'%output_talbe)
        hiveContext.sql('create table %s as select * from tmptable'%output_talbe)
    elif sys.argv[1]=='-usertfidf_brand':
        i=1
        top_freq=int(124706*2.5)
        min_freq=int(sys.argv[i+1])
        limit=int(sys.argv[i+2])
        feed_ds=sys.argv[i+3]
        output_talbe=sys.argv[i+4]
        index_file='/user/zlj/need/vocab_index'
        # rdd_pre = hiveContext.sql(sql_tfidfbrand%feed_ds).map(lambda x: (x.user_id, title_clean(x[1]))).coalesce(100)
        rdd_pre = hiveContext.sql('select * from t_zlj_feed_tag_0901 limit 1000').map(lambda x: (x.user_id, title_clean(x[1]))).coalesce(100)
        # rdd_pre.map(lambda x:" ".join(x[1])).saveAsTextFile('/user/zlj/corpus')

        rst=tfidf(rdd_pre,top_freq=top_freq,min_freq=min_freq,limit=limit,index_file=index_file)
        df=hiveContext.createDataFrame(rst,schema)
        hiveContext.registerDataFrameAsTable(df, 'tmptable')
        # hiveContext.sql('drop table if EXISTS  t_zlj_userbuy_item_tfidf_tags')
        # hiveContext.sql('create table t_zlj_userbuy_item_tfidf_tags as select * from tmptable')
        hiveContext.sql('drop table if EXISTS  %s'%output_talbe)
        hiveContext.sql('create table %s as select * from tmptable'%output_talbe)
    elif sys.argv[1]=='-aliseg':
        i=1
        top_freq=int(3981121)
        min_freq=int(sys.argv[i+1])
        limit=int(sys.argv[i+2])
        feed_ds=sys.argv[i+3]
        output_talbe=sys.argv[i+4]
        # rdd_pre = hiveContext.sql(sql_tfidfbrand%feed_ds).map(lambda x: (x.user_id, title_clean(x[1]))).coalesce(100)
        rdd_pre =sc.textFile('/user/zlj/project/termweight/joininfo/part-00000').map(lambda x:x.split('\001')).map(lambda x: (x[0], title_clean_ali(x[1]))).coalesce(100)
        # rdd_pre.map(lambda x:x[1]).flatMap(lambda x:x).filter(lambda x: x.find('_')).countByKey()
        # rdd_pre.map(lambda x:" ".join(x[1])).saveAsTextFile('/user/zlj/corpus')
        index_file='/user/zlj/project/termweight/init_word_index_count'
        rst=tfidfali(rdd_pre,top_freq=top_freq,min_freq=min_freq,limit=limit,index_file=index_file)
        df=hiveContext.createDataFrame(rst,schema)
        hiveContext.registerDataFrameAsTable(df, 'tmptable')
        hiveContext.sql('drop table if EXISTS  %s'%output_talbe)
        hiveContext.sql('create table %s as select * from tmptable'%output_talbe)

    elif sys.argv[1]=='-item':
        i=1
        min_freq=int(sys.argv[i+1])
        limit=int(sys.argv[i+2])
        feed_ds=sys.argv[i+3]
        input_table=sys.argv[i+4]
        input_table_docid=sys.argv[i+5]
        input_table_title=sys.argv[i+6]
        output_talbe=sys.argv[i+7]

        sql_itemtitle='''
        select
         %s,
         %s
        from
        %s
        '''
        # hmm_ds=sys.argv[4]
        rdd_pre = hiveContext.sql(sql_itemtitle%(input_table_docid,input_table_title,input_table)).map(lambda x: (x[0], [i for i in x[1].split() if len(i) > 1]))
        rst=tfidf(rdd_pre,top_freq=1000,min_freq=100,limit=5)
        df=hiveContext.createDataFrame(rst,schema)
        hiveContext.registerDataFrameAsTable(df, 'tmptable')
        hiveContext.sql('drop table if EXISTS  %s'%output_talbe)
        hiveContext.sql('create table %s as select * from tmptable'%output_talbe)
    elif sys.argv[1]=='-corpus':
        i=1
        min_freq=int(sys.argv[i+1])
        limit=int(sys.argv[i+2])
        feed_ds=sys.argv[i+3]
        output_talbe=sys.argv[i+4]
        rdd_pre = hiveContext.sql(sql_tfidf%feed_ds).map(lambda x: (x.user_id, [i.split('_')[0] for i in x[1].split() if len(i.split('_')[0])>1]))
        words = set(rdd_pre.map(lambda x: tcount(x[1])).flatMap(lambda x: x).coalesce(100).reduceByKey(lambda a,b:a+b).filter(lambda x: x[1] > min_freq).map(lambda x: x[0]).collect())
        # wordmap={}
        # for  index,value in enumerate(words,1):
        #     wordmap[value]=index
        broadcastVar = sc.broadcast(words)
        dict = broadcastVar.value
        rdd = rdd_pre.map(lambda (x, y): (x, [i for i in y if i in dict])).map(lambda (x,y):x+"\t"+" ".join([ i[0]+":"+str(i[1]) for i in tcount(y)]))
        rdd.saveAsTextFile("/user/zlj/temp/user_corpus")
        # sc.textFile('/user/zlj/project/termweight/init_word_index_count').map(lambda x:int(x.split()[0])).max()