#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')


end_flag=0
buf=''
# for line in open("E:\\work\\xinzhengxin\\oulaiya_ xinghao_1.txt"):
#     if 'END' in line:
#         print buf.decode('utf-8')
#         buf=''
#         end_flag=0
#         buf=line.strip()
#     else: buf=buf+'  '+line.strip()

map={}
for line in open("E:\\work\\xinzhengxin\oulaiya.txt"):
    k,v=line.split()
    # print type(v)
    # print v
    map[v]=k
# print map

print(len(map.items()))

for line in open("E:\\work\\xinzhengxin\\oulaiya_ xinghao_1_dealclean.txt"):
    ls=line.split()

    # print ls
    if(len(ls)!=1):
        k=ls[0]
        # print k
        v=ls[1:]
        if map.has_key(k.decode('utf-8').encode('utf-8')):print map[k],k.decode('utf-8')," ".join(v).decode('utf-8')



from pyspark.sql import *
import sys
from pyspark import SparkContext

import rapidjson as json

'''
使用脚本
spark-submit  --total-executor-cores  100   --executor-memory  20g  --driver-memory 20g  1_item_inc.py  -inc  /commit/iteminfo/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-02  20151101 20151102
'''
# /data/develop/ec/tb/iteminfo/jiu.iteminfo


sc=SparkContext(appName="iter_inc")

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
path=sys.argv[1]

sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_wine').map(lambda x:x.split('\001')).map(lambda x:' '.join([x[0],x[12]+" BIG",x[1]+" END"]))\
    .saveAsTextFile('/user/zlj/temp/tb_wine_title')


def fun(x):
    brand=''
    lv=[]
    for key in x:
        word =key.split('[|')[0]
        if u'产品-品牌' in key:
            lv.append((word,'6'))
        elif u'产品类型-简单' in key:
            lv.append((word,'6'))
        elif u'NUM' in key:
            lv.append((word,'3'))
        elif u'度' in key:
            lv.append((word,'3'))
        elif u'产品-型号' in key:
            lv.append((word,'5'))
        elif u'英文缩略' in key:
            lv.append((word,'3'))
        else:
            lv.append((word,'1'))
    return ['\001'.join(['_'.join(i) for i in lv])]



def fun_x(x):
    if len(x)!=2:return None
    info,title=x
    kv=info.split('\001')
    item_id=kv[0].split('[|')[0]
    brand=''.join([i.split('[|')[0] for i in kv[1:] if len(i)>1])
    ls=title.split('\001')
    lv=[]
    for key in ls:
        if len(key)<2:continue
        word =key.split('[|')[0]
        if u'产品-品牌' in key:
            lv.append((word,'6'))
        elif u'产品类型-简单' in key:
            lv.append((word,'6'))
        elif u'NUM' in key:
            lv.append((word,'3'))
        elif u'度' in key:
            lv.append((word,'3'))
        elif u'产品-型号' in key:
            lv.append((word,'5'))
        elif u'英文缩略' in key:
            lv.append((word,'3'))
        else:
            lv.append((word,'1'))
    return [item_id,brand,'\001'.join(['_'.join(i) for i in lv])]

def fun_try(x):
    try:
        return fun_x(x)
    except:
        return None


# sc.textFile('/user/zlj/temp/tb_wine_title_cut_w').map(lambda x:len(x.split('BIG'))).groupByKey().mapValues(len).take(10)

sc.textFile('/user/zlj/temp/tb_wine_title_cut_w').map(lambda x:fun_x(x.split('BIG'))).filter(lambda x: x is not None).count()


sc.textFile('/user/zlj/temp/tb_wine_title_cut_w').map(lambda x:fun_x(x.split('BIG'))).\
    filter(lambda x: x is not None).map(lambda x:(x[0],x)).groupByKey(10).map(lambda (x,y):list(y)[0])\
    .map(lambda x:'\t'.join(x)).saveAsTextFile('/user/zlj/temp/tb_wine_title_cut_w_clean_groupby')


sc.textFile('/user/zlj/temp/tb_wine_title_cut_w_clean_groupby').count()


rdd=sc.textFile('/user/zlj/temp/tb_wine_title_cut_w_clean').map(lambda x:x.split()).map(lambda x:(x[0],x)).groupByKey(10).map(lambda (x,y):list(y)[0])
rdd.map(lambda x:'\t'.join(x)).saveAsTextFile('/user/zlj/temp/tb_wine_title_cut_w_clean_groupby')

# -------------------------------------------------

sc.textFile('/user/zlj/temp/jingdong_jiu').map(lambda x:x.split('\001')).\
    map(lambda x:' '.join([x[1],x[9]+'BIG',x[6]+x[12]+"END"])).saveAsTextFile('/user/zlj/temp/jd_wine_title')


def fun_jdx(x):
    if len(x)!=2:return None
    info,title=x
    kv=info.split('\001')
    item_id=kv[0].split('[|')[0]
    brand=''
    if u'旗舰店' in info:
        brand=''.join([i.split('[|')[0] for i in kv[1:] if len(i)>1 and u'产品-品牌' in i])
    else:
        brand=''.join([i.split('[|')[0] for i in kv[1:] if len(i)>1])
    ls=title.split('\001')
    lv=[]
    for key in ls:
        if len(key)<2:continue
        word =key.split('[|')[0]
        if u'产品-品牌' in key:
            lv.append((word,'6'))
        elif u'产品类型-简单' in key:
            lv.append((word,'6'))
        elif u'NUM' in key:
            lv.append((word,'3'))
        elif u'度' in key:
            lv.append((word,'3'))
        elif u'产品-型号' in key:
            lv.append((word,'5'))
        elif u'英文缩略' in key:
            lv.append((word,'3'))
        else:
            lv.append((word,'1'))
    return [item_id,brand,'\001'.join(['_'.join(i) for i in lv])]




sc.textFile('/user/zlj/temp/jd_wine_title_cut_w').map(lambda x:fun_jdx(x.split('BIG'))).\
    filter(lambda x: x is not None).map(lambda x:'\t'.join(x)).saveAsTextFile('/user/zlj/temp/jd_wine_title_cut_w_clean_groupby')