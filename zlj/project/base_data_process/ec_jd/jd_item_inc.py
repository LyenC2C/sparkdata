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
def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content
def try_parse(line,flag):
    try:
        return parse(line,flag)
    except:return None


def p(line):
    ob=json.loads(valid_jsontxt(line))
    item_id=ob['skuid']
    product=ob['product']
    item_title=ob['title']
    brand_id=product['brand']
    table=ob['table']
    table_map=[]
    # for  i in table:
    #     key=i['key']
    #     value=i['value']
    #     table_map.append(key+":"+value)
    u_jd=ob.get('u_jd','-')
    p_price=ob['price']['p']
    m_price=ob['price']['m']
    cat1=ob['cate1']
    cat2=ob['cate2']
    cat3=ob['cate3']
    cat1_name=''
    cat2_name=''
    cat3_name=''
    detail=ob['detail']
    detail_map=[]
    for  i in detail:
        key=i['key']
        value=i['value']
        detail_map.append(key+":"+value)
    return [item_id,item_title,brand_id,u_jd,p_price,m_price,cat1,cat1_name,cat2,cat2_name,cat3,cat3_name,','.join(detail_map),'\t'.join(table_map)]


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
    favor=itemInfoModel.get('favcount','-')
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
    elif sys.argv[1]=='-inc':
        filepath=sys.argv[2]
        ds_1=sys.argv[3]
        ds=sys.argv[4]
        rdd=sc.textFile(filepath,100).map(lambda x:try_parse(x,'inc')).filter(lambda x: x is not None)
        hiveContext.sql('use wlbase_dev')
        df=hiveContext.sql('select * from t_base_ec_item_dev where ds=%s'%ds_1)
        schema1=df.schema
        rdd1=df.map(lambda x:(x.item_id,[x.item_id,x.title,x.cat_id,x.cat_name,x.root_cat_id,x.root_cat_name,x.brand_id,x.brand_name,
                                     x.bc_type,x.price,x.price_zone,x.is_online,x.off_time,x.favor,x.seller_id,x.shop_id,x.location, x.ts]))
        rdd2=rdd.union(rdd1).groupByKey()
        rdd3=rdd2.map(lambda (x,y):fun_sorted(y)).coalesce(40)
        ddf=hiveContext.createDataFrame(rdd3.map(lambda x:fun1(x,ds)),schema1)
        hiveContext.registerDataFrameAsTable(ddf,'tmptable')
        # sql='''
        # insert overwrite table t_base_ec_item_dev partition(ds=%s)
        # s
        # '''
        hiveContext.sql(sql_insert%(ds))

# for i in rs:
#     if  i =='\\N':
#         print len(i)