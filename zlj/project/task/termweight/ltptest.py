# encoding: utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

''.startswith()
root='/home/hadoop/common/segfile/tlpdata'
from pyltp import Segmentor
segmentor = Segmentor()
segmentor.load(root+'cws.model')


sc.textFile('/user/zlj/temp/100line_title').map(lambda x: ' '.join(segmentor.segment(x))).saveAsTextFile('/user/zlj/temp/100line_title_ltp')


''.replace('.','',1).isdigit()
rs=sc.textFile('/user/zlj/t_zlj_feed_tag_0901_clean/')\
    .map(lambda x:x.split()).flatMap(lambda x: x)\
    .filter(lambda x: not x.replace('.','',1).isdigit() ).map(lambda x:(x,1)).countByKey()


rdd=sc.parallelize(rs.items()).map(lambda x:x[0]+"\t"+str(x[1])).saveAsTextFile('/user/zlj/t_zlj_feed_tag_0901_clean_count')
d={}
for k,v in d:
sc.parallelize()

json.dump(rs).
not str(x[0]).replace('.','',1).isdigit()