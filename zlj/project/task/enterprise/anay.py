#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
import time
import rapidjson as json

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
from pyspark.sql import SQLContext, Row

rdd=sc.textFile('/user/zlj/enterprise/Baseinfo.samename.id.id.fiter_two')

# rdd.count()
# [].append()
# rdd.map(lambda x:x.replace('(','').replace(')','').split(',')).map(lambda x:x[0].split('\\t')).take(1)


# 去重后 交际大于2 的 410231
# 去除bug 结果      252303
# bug是  英文名字
# 逸可信息技术（上海）有限公司    310000400614881 BARRY HURLEY    WILHELM FRIEDRICH MITTRICH      THOMAS REISS    何山逸


# 投资关系 160334

# 已经在关系中的9720
# 去除现有的投资关系  共有  400403  （数据可能有问题）

# rdd=sc.textFile('/user/zlj/enterprise/Investment.invid.invid').map(lambda x:'\t'.join(sorted(x.split(),reverse=True)))
#
#
# rddt=sc.textFile('/user/zlj/enterprise/Baseinfo.name.id.employees.fiternoEmploy').filter(lambda x:'310000400614881' in x).take(10)

rdd=sc.textFile('/user/zlj/temp/Company.Employees').repartition(100).\
    map(lambda x:[i.strip() for i in x.split('\t')]).filter(lambda x:len(x)>2 and x[1].isdigit()).map(lambda x:(x[1],x))\
    .groupByKey().map(lambda (x,y):'\t'.join(list(y)[0])).saveAsTextFile('/user/zlj/enterprise/Company.Employees')



sc.textFile('/user/zlj/temp/Company.Employees').repartition(100).\
    map(lambda x:[i.strip() for i in x.split('\t')]).map(lambda x:(x[0],1)).groupByKey().count()

rdd=sc.textFile('/user/zlj/enterprise/Company.Employees').repartition(100).map(lambda x:[i.strip() for i in x.split('\t')]).filter(lambda x:len(x)>2 and x[1].isdigit())\
    .map(lambda x:[(i,x[1]) for i in set(x[2:]) if len(i)>2 and len(i)<10 ]).flatMap(lambda x:x)

rdd.map(lambda x:(x[0],1)).reduceByKey(lambda a,b:a+b).map(lambda (x,y):x+'\t'+str(y)).saveAsTextFile('/user/zlj/enterprise/name1')
import itertools

rdd2=rdd.groupByKey().map(lambda (x,y):[i for i in itertools.combinations(list(y), 2)])

rdd2.filter(lambda x: len(x)==2 and x[0]!=x[1]).map(lambda x:'\t'.join(x)).saveAsTextFile('/user/zlj/enterprise/Baseinfo.samename.id.id')

rdd2=rdd.join(rdd).map(lambda (x,y):sorted(y,reverse=True)).filter(lambda x: len(x)==2 and x[0]!=x[1] )
rdd2.map(lambda x:'\t'.join(x)).saveAsTextFile('/user/zlj/enterprise/Baseinfo.samename.id.id')

rs=sc.textFile('/user/zlj/enterprise/Baseinfo.samename.id.id').map(lambda x:('\t'.join(sorted(x.split(),reverse=True)),1)).reduceByKey(lambda a,b:a+b)

rs.filter(lambda x:x[1]>2).map(lambda x:x[0]+'\t'+str(x[1])).saveAsTextFile('/user/zlj/enterprise/Baseinfo.samename.id.id.fiter_three')





infer=sc.textFile('/user/zlj/enterprise/Baseinfo.samename.id.id.fiter_three').map(lambda x:x.split()).map(lambda t:Row(id1=t[0], id2=t[1],w=int(t[2])))

invest=sc.textFile('/user/zlj/enterprise/Investment.invid.invid/').map(lambda x:x.split()).map(lambda t:Row(id1=t[0], id2=t[1],w=1))

schemainfer = sqlContext.createDataFrame(infer)
schemainfer.registerTempTable("infer")

schemainvest= sqlContext.createDataFrame(invest)
schemainvest.registerTempTable("invest")

sqlContext.sql('select * from infer').count()
rdd11=sqlContext.sql("select id1, id2, w from  (SELECT t1.*,t2.id1 idss  FROM infer t1 left join invest t2 on t1.id1=t2.id1 and t1.id2=t2.id2 )t  where idss is null ")
# 大于2的关系总数有 10606496

# 包换现有关系  15132

# bug 重复 问题
# 天津兰德智川投资有限公司        120225000032440 陈亮    张连喜  韩智
# 天津兰德智川投资有限公司        120225000032440 陈亮    张连喜  韩智

rdd11.map(lambda x: x[0]+'\t'+x[1]+'\t'+str(x[2]/2)).saveAsTextFile('/user/zlj/enterprise/Baseinfo.samename.id.id.fiter_three_filter_invest')

# 1276276  人名重合大于2的

# 1261873  过滤投资关系数据

# invest 160078


emp=sc.textFile('/user/zlj/enterprise/Baseinfo.name.id.employees.fiternoEmploy/').map(lambda x:x.split('\t')).map(lambda x:Row(id=x[1],info='\001'.join(x)))
schemaemp = sqlContext.createDataFrame(emp)
schemaemp.registerTempTable("emp")

sqlContext.sql('select * from infer limit 10').take(10)

infer=sc.textFile('/user/zlj/enterprise/Baseinfo.samename.id.id.fiter_three_filter_invest').map(lambda x:x.split()).map(lambda t:Row(id1=t[0], id2=t[1],w=int(t[2])))
schemainfer = sqlContext.createDataFrame(infer)
schemainfer.registerTempTable("infer")


sql='''

select id1 ,id2 ,w, t3.info as t1info ,t4.info as t2info
 from
(select t1.id1 ,t2.info,t1.id2,w  from  infer t1
join
  emp t2 on t1.id1=t2.id
 )
t3
  join emp t4 on  t3.id2=t4.id

'''

rddtest=sqlContext.sql(sql)

rddtest.map(lambda x:[x.id1,x.id2,str(x.w),'_'.join(x.t1info.split('\001')),'_'.join(x.t2info.split('\001'))])\
    .map(lambda x:'\t'.join(x)).saveAsTextFile('/user/zlj/enterprise/Baseinfo.samename.id.id.fiter_three_filter_invest_joininfo')
