# encoding: utf-8
__author__ = 'zlj'
# corpus /user/zlj/tmp/title
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from jpype import *
import jpype
sys.setrecursionlimit(10000)
vmPath = jpype.getDefaultJVMPath()
jpype.startJVM(vmPath, "-Xms320m", "-Xmx1024m","-mx1024m","-Djava.class.path=/home/hadoop/common/segfile/hanlp-1.2.7.jar:")
JDClass = JClass("com.hankcs.hanlp.seg.CRF.CRFSegment")
HJDClass = JClass("com.hankcs.hanlp.HanLP")
HJDClass.setRoot("/home/hadoop/common/segfile/")
jd = JDClass().enableNameRecognize(True)

# s="努西娜 2015秋冬平底短靴女真皮厚底靴子马"
# print jd.seg((s))
#print jd.seg(jpype.JString(s))

sc = SparkContext(appName="cmt")
# sqlContext = SQLContext(sc)
# hiveContext = HiveContext(sc)

rdd=sc.textFile('/user/zlj/tmp/title').map(lambda x:" ".join([str(i.word) for i in jd.seg(jpype.JString(x))]))
rdd.saveAsTextFile('/user/zlj/tmp/title_seg')