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

# 有专业 但是没发现学校的群


qun=sc.textFile('/user/zlj/qq/qqgroup_school').map(lambda x:(x.split('\001')[0],1))

member=sc.textFile('/user/wrt/qun_member_info').map(lambda x:(x.split('\001')[-2],x.split('\001')[0]))


rs=qun.join(member)

wjqunmember=rs.map(lambda (x,y):(list(y)[-1],list(y)[0])).repartition(50)

wjqunmember.map(lambda x:x[0]).saveAsTextFile('/user/zlj/qq/qqgroup_school_qqid')

member.map(lambda x:x[-1]+'\t'+x[0]).saveAsTextFile('/user/zlj/qq/qqid_qun')


st=member.map(lambda x:(x[-1],x[0])).repartition(50).join(wjqunmember).map(lambda (x,y):y)



st.take(10)
reduce_rs=st.reduceByKey(lambda a,b:a+b)

reduce_rs.take(10)

conf=sc._conf