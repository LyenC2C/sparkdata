#coding:utf-8
__author__ = 'zlj'
import sys

'''
word index
join_count

join_user_save

'''

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext(appName="join_count")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

path="/user/zlj/project/termweight/"
import sys
if __name__ == "__main__":
    hiveContext.sql('use wlbase_dev')
    if sys.argv[1]=='-count':

        userbuy_rdd=hiveContext.sql('select item_id,user_id from t_zlj_t_base_ec_item_feed_dev_2015_iteminfo_t limit 10000').map(lambda x:(int(x[0]),int(x[1])))
        word_index_rdd=sc.textFile('/user/zlj/data/aliwordseg_wordindex_1225').map(lambda x:x.split()).map(lambda x:(x[0],int(x[2]))).collectAsMap()
        broadcastVal=sc.broadcast(word_index_rdd)
        word_index_dic=broadcastVal.value
        cut=hiveContext.sql('select item_id,title_cut from t_base_ec_item_title_cut where ds=20151225 limit 10000000')
        # item_word=cut.map(lambda x:(x[0],x[1])).filter(lambda x:x[0].isdigit()).map(lambda x:(int(x[0]),[ i for i in x[1].split()]))
        #                                                                             ,[word_index_dic[i] for i in x[1].split() if len(i)>1])))

        item_word=hiveContext.sql('select item_id,title_cut from t_base_ec_item_title_cut where ds=20151225 limit 10000000')\
            .cut.map(lambda x:(int(x[0],[word_index_dic[i] for i in x[1].split() if len(i)>1])))


        joinrdd=item_word.join(userbuy_rdd)

        index_word_dic = dict(zip(word_index_dic.values(),word_index_dic.keys()))
        word_count_rdd=userbuy_rdd.map(lambda (x,y):[i for i in y][0]).flatMap(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
        word_count_rdd.map(lambda (x,y):" ".join( str(i) for i in [index_word_dic[x],x,y])).saveAsTextFile(path+'title_coutn_index')

        def join(y):
            lv=[]
            for i in y:
                lv.append("\002".join([str(j) for j in i]))
            return "\003".join(lv)
        userbuy_rdd.map(lambda (x,y):[i for i in y]).map(lambda x:(x[0],x[1])).groupByKey().map(lambda (x,y):str(x)+"\001"+join(y)).saveAsTextFile(path+"user_buy_index")






# rdd1=sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_userbuy_0701/').map(lambda x: x.split('\001'))\
#     .filter(lambda x: x[0].isdigit and x[1].isdigit()).map(lambda x:(int(x[0]),int(x[1])))
# rdd2=sc.textFile('/user/zlj/project/termweight/title_corpus_aliseg_1206_clean_index/').map(lambda x:x.split('\001'))\
#     .filter(lambda x:x[0].isdigit() and len(x[0])>1)\
#     .map(lambda x:(int(x[0]),[int(i) for i in x[1].split() if i.isdigit() and len(i)>1]))
# rdd=rdd1.join(rdd2)
# def tcount(ls):
#     lv=[]
#     for i in ls:
#         lv.append('\002'.join([str(k) for k in i]))
#     # for i in s:
#     #     re.append(str(i)+"\002"+str(lv.count(i)))
#     return '\003'.join(lv)
# rdd.map(lambda (x,y):(y[0],y[1])).groupByKey().map(lambda (x,y):str(x)+"\001"+tcount(y)).saveAsTextFile('/user/zlj/project/termweight/joininfo')





# rdd=sc.textFile('/user/zlj/project/termweight/joininfo/').map(lambda x:x.split('\001')[1].split('\003'))\
#     .flatMap(lambda x:x).map(lambda x:x.split('\002')).filter(lambda x: len(x[0])>0)\
#     .map(lambda  x: (int(x[0]),int(x[1])))\
#     .reduceByKey(lambda a,b:a+b)
#
# indexrdd=sc.textFile('/user/zlj/project/termweight/init_word_index').map(lambda x:x.split('\001')).map(lambda x:(x[0],x[1]))
#
# broadcastVar = sc.broadcast(indexrdd.collectAsMap())
# wdic = broadcastVar.value
# rs=rdd.map(lambda (x,y):str(x)+"\t"+wdic.get(str(x).decode('utf-8'),'')+"\t"+str(y))
# rs.saveAsTextFile('/user/zlj/project/termweight/init_word_index_count')

