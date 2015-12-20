#coding:utf-8
__author__ = 'zlj'



# import json
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
# def valid_jsontxt(content):
#     if type(content) == type(u""):
#         return content.encode("utf-8")
#     else :
#         return content
def parse_price(price_dic):
    min=1000000000.0
    price='0'
    price_range='-'
    for value in price_dic:
        tmp=value['price']
        v=0
        if '-' in tmp:     
            v=(float(tmp.split('-')[0])+float(tmp.split('-')[1]))/2.0
        else :           
            v=float(tmp)
        if min>v:
            min=v
            price=v
            if '-' in tmp: price_range=tmp
    return [price,price_range]
def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
        return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    else: return res
def try_parse(line,flag):
    try:
        return parse(line,flag)
    except:return None
def parse(line,flag):
    ts=''
    txt=''
    if flag=='insert':
        txt=valid_jsontxt(line)
    else:
        lis=valid_jsontxt(line).split('\t')
        if len(lis)!=3:
            return
        ts=lis[0]
        txt=lis[2]
    ob=json.loads(txt)
    # line=valid_jsontxt(line_s)
    # ts,id,txt=line.split('\t')
    # ob=json.loads(txt)
    if type(ob)==type(0.0):
        return None
    itemInfoModel=ob['itemInfoModel']
    location=valid_jsontxt(itemInfoModel.get('location','-'))
    item_id=itemInfoModel.get('itemId','-')
    title=itemInfoModel.get('title','-')
    categoryId=itemInfoModel.get('categoryId','-')
    cat_name='-'
    root_cat_id='-'
    root_cat_name='-'
    favor=itemInfoModel.get('favcount','0')
    if not favor.isdigit(): favor='0'
    trackParams=ob['trackParams']
    BC_type=trackParams.get('BC_type','-')
    brandId=trackParams.get('brandId','-')
    brand_name='-'
    props=ob.get('props',[])
    for v in props:
        if valid_jsontxt('品牌') in valid_jsontxt(v['name']):
            brand_name=v['value']
    value=parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    price=value[0]
    price_zone=value[1]
    is_online=1
    off_time='-'
    seller=ob['seller']
    seller_id=seller.get('userNumId','-')
    shopId=seller.get('shopId','-')
    # ts=time.time()
    list=[]
    list.append(item_id)
    list.append(title)
    list.append(categoryId)
    list.append(cat_name)
    list.append(root_cat_id)
    list.append(root_cat_name)
    list.append(brandId)
    list.append(brand_name)
    list.append(BC_type)
    list.append(str(price))
    list.append((price_zone))
    list.append((is_online))
    list.append(off_time)
    list.append(int(favor))
    list.append(seller_id)
    list.append(shopId)
    list.append(location)
    # list.append(str(int(time.time())))
    list.append((ts))
    strlist=[]
    if flag=='insert':
        # for i in list:
        #     if len(i)==0: strlist.append('-')
        #     else : strlist.append(i)
        # return "\001".join(strlist)
        return list
    elif flag=='inc':
        return (item_id,list)


# rdd=sc.textFile('/data/develop/ec/tb/iteminfo_new/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-01',100)\
def fun_sorted(y):
    return sorted(y,key=lambda t : t[-1],reverse=True)[0]
def f_coding(x):
    if type(x) == type(""):
        return x.decode("utf-8")
    else:
        return x

def fun1(x,ds):
    x.append(ds)
    return [f_coding(i) for i in x]

sql_insert='''
insert  OVERWRITE table t_base_ec_item_dev PARTITION(ds=%s)
  SELECT  /*+ mapjoin(t2)*/
t1.item_id,
t1.title,
t1.cat_id,
t2.cate_name as cat_name,
t2.cate_level1_id as root_cat_id,
t2.cate_level1_name as root_cat_name,
t1.brand_id,
t1.brand_name,
t1.bc_type,
t1.price,
t1.price_zone,
t1.is_online,
t1.off_time,
t1.favor,
t1.seller_id,
t1.shop_id,
t1.location,
t1.ts
  from
(
SELECT
cate_id,
cate_name,
cate_level1_id,
cate_level1_name
FROM
t_base_ec_dim
where  ds=20151023
)t2 join  tmptable  t1 on t1.cat_id=t2.cate_id
'''
if __name__ == "__main__":
    hiveContext.sql('use wlbase_dev')
    if sys.argv[1] == '-h':
        comment = '-新增商品 \n\
				  '
        print comment
        print '-insert argvs:\n argv[1]:file or dir input\n argv[2]:ds  \n'
        print '-inc      argvs:\n argv[1]:file or dir input\n argv[2]:ds_1  \n argv[3] ds\n'
    elif sys.argv[1]=='-insert':
        filepath=sys.argv[2]
        ds=sys.argv[3]
        rdd=sc.textFile(filepath,100)\
            .map(lambda x:parse(x,'insert')).filter(lambda x: x is not None)\
            .map(lambda x:fun1(x,ds))
        df=hiveContext.sql('select * from t_base_ec_item_dev limit 1')
        schema1=df.schema
        ddf=hiveContext.createDataFrame(rdd,schema1)
        hiveContext.registerDataFrameAsTable(ddf,'tmptable')
        # sql='''
        # insert overwrite table t_base_ec_item_dev partition(ds=%s)
        # select * from tmptable
        # '''
        hiveContext.sql(sql_insert%(ds))

    elif sys.argv[1]=='-inc':
        filepath=sys.argv[2]
        ds_1=sys.argv[3]
        ds=sys.argv[4]
        rdd=sc.textFile(filepath,100).map(lambda x:try_parse(x,'inc')).filter(lambda x: x is not None)
        hiveContext.sql('use wlbase_dev')
        df=hiveContext.sql('select * from wlbase_dev.t_base_ec_item_dev where ds=%s limit 100'%ds_1)
        #df=hiveContext.sql('select * from t_base_ec_item_dev where ds=%s '%ds_1)
        schema1=df.schema
        # rdd1=df.map(lambda x:(x.item_id,[x.item_id,x.title,x.cat_id,x.cat_name,x.root_cat_id,x.root_cat_name,x.brand_id,x.brand_name,
        #                              x.bc_type,x.price,x.price_zone,x.is_online,x.off_time,x.favor,x.seller_id,x.shop_id,x.location, x.ts]))
        rdd1=df.map(lambda x:(x.item_id,[x.item_id,x.title,x.cat_id,x.cat_name,x.root_cat_id,x.root_cat_name,x.brand_id,x.brand_name,
                                     x.bc_type,x.price,x.price_zone,x.is_online,x.off_time,x.favor,x.seller_id,x.shop_id,x.location, x.ts]))
        rdd_dev=rdd1.filter(lambda x: x[0].isdigit()).map(lambda (x,y):(int(x),y))
        rdd_in=rdd.map(lambda (x,y):(int(x),y)).groupByKey().map(lambda (x,y):(x,[i for i in y][0]))
        rddjoin=rdd_in.fullOuterJoin(rdd_dev)
        def fun(x,y):
            lv=[]
            ts=[]
            t1,t2=y
            if t1 is None:ts=t2
            elif t2 is None:ts=t1
            if t1 is not None and t2 is not None:
                if int(t1[-1])>int(t2[-1]): ts=t1 
                else: ts=t2
            ts.append(ds)
            return ts 
        
        rdd3=rddjoin.map(lambda (x,y):fun(x,y)).filter(lambda x:x is not  None).coalesce(100)
        rdd3.map(lambda x:'\001'.join([str(valid_jsontxt(i)) for i in  x])).saveAsTextFile('/user/zlj/data/temp/t_base_ec_item_dev_tmp')
        #hiveContext.sql('use wlbase_dev')
        #ddf=hiveContext.createDataFrame(rdd3,schema1)
        #ddf.saveAsTable('t_base_ec_item_dev',mode='overwrite',partitionBy='2015')
        #hiveContext.registerDataFrameAsTable(ddf,'tmptable')
        #hiveContext.sql(sql_insert%(ds))



# hiveContext.sql('use wlbase_dev')
# ds='20151104'
# df=hiveContext.sql('select * from t_base_ec_item_dev where ds=%s'%ds)
# schema1=df.schema
#
# rdd1=df.map(lambda x:(x.item_id,[x.item_id,x.title,x.cat_id,x.cat_name,x.root_cat_id,x.root_cat_name,x.brand_id,x.brand_name,x.bc_type,x.price,x.price_zone,x.is_online,x.off_time,x.favor,x.seller_id,x.shop_id,x.ts]))
#
#
# rdd3.map(lambda x:"\001".join([str(valid_jsontxt(i)) for i in x])).saveAsTextFile("/data/develop/ec/tb/iteminfo_tmp/1101.dir")

    # return x



# sql='''
# insert overwrite table t_base_ec_item_dev partition(ds=%s)
# select item_id,title,cat_id,cat_name,root_cat_id,root_cat_name,brand_id,brand_name,bc_type,price,price_zone,is_online,off_time,favor,seller_id,shop_id,ts from testtable
# '''

