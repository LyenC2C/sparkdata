#coding:utf-8
import sys
#import urllib
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkContext
def f(x):
    ss = x.split('\t')
    u = ss[0]
    b = ss[5]
    a = ""
    #u = u.decode('UTF-8')
    for uchar in u:
        if (uchar >= u'\u4e00' and uchar<= u'\u9fa5') or (uchar >= u'\u0030' and uchar<=u'\u0039') or \
            (uchar >= u'\u0041' and uchar<=u'\u005a') or (uchar >= u'\u0061' and uchar<=u'\u007a') or \
            (uchar == '\t') or (uchar == '-') :
			a += uchar#.encode('utf-8')
    if len(a) > 1:
        return a + "\t" + b
    else:
        return None


if __name__ == "__main__":
    sc=SparkContext(appName="keyword_title")
    rdd = sc.textFile(sys.argv[1])
    rdd.map(lambda x:f(x)).filter(lambda x:x!=None).saveAsTextFile('/user/zlj/wrt/test/'+sys.argv[2])

