# encoding: utf-8
__author__ = 'zlj'

from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *

hiveContext = HiveContext(sc)

schema = StructType([
	StructField("item_id",StringType(), True)
	])

rdd=sc.textFile('/user/zlj/indb.itemid.uniq').map(lambda x: (str(x)))
df = hiveContext.createDataFrame(rdd , schema)
hiveContext.sql('use wlbase_dev')
hiveContext.registerDataFrameAsTable(df,'tmptable')

sql='''
select
t2.item_id ,t2.price
from
tmptable t1 join
(
select item_id ,price
from
t_base_ec_item_dev
where ds=20151107
) t2
on t1.item_id =t2.item_id
'''
hiveContext.sql(sql).map(lambda x: '\t'.join([x.item_id,str(x.price)])).saveAsTextFile('indb.itemid.uniq.price')


file_blk={}
blk_path={}

for line in open():
	file ,id=line.split()
	file_blk[id]=file

for line in open():
	path ,id=line.split()
	file_blk[id]=path


d1="sed -i '1d'  %s"
dend="sed -i '$d' %s"

import  os
os.system(d1%'')
fw=open('w','w')
for k,v in file_blk.iteritems():
	ids=v.split()
	for index,id in enumerate(ids):
		if id in blk_path:
			fw.write('\t'.join([k,id,blk_path[id]]))
		# else
