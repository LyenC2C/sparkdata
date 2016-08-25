#coding:utf-8
import sys
from pyspark import *

'''
0		tb_id	string
1		tage	int
2		tgender	string
3		city	string
4		alipay	int
5		year	double
6		buycnt	string
7		verify_level	int
8		ac_score_normal	double
9		sum_level	int
10		feedrate	double
'''

'''
def format_info(line):
    ls = line.replace(","," ").replace("|"," ").split("\001")
    if len(ls) != 16:
        return
    #形成rowkey,userid_828000_
    rowkey = ls[2]+'_'+ls[5][-3:]+ls[1][-3:]
    v = '|'.join([ls[5],ls[8],ls[9],ls[12],ls[14],ls[13]])
    return rowkey+','+v+','+ls[0]
'''

#新record表,建立线上库所需数据
def format_info(line):
    ls = line.replace(",","").replace("|"," ").replace("\N",'').split("\001")
    if len(ls) < 10 or len(ls[0]) <2 or len(ls[2]) > 15 :
        return None
    rowkey = ls[0]
    v_info = ','.join([ls[1],ls[2],ls[3],ls[4],ls[5],ls[6],ls[7],ls[8][:5],ls[9],ls[10][:5]])
    sum = 0
    for each in ls[1:]:
        if each != '':
            sum += 1
    v_sum = str(sum)
    return rowkey+','+v_info+','+v_sum

if __name__ == '__main__':
    '''
    conf = SparkConf()
    conf.set("spark.kryoserializer.buffer.mb","512")
    conf.set("spark.broadcast.compress","true")
    conf.set("spark.driver.maxResultSize","4g")
    conf.set("spark.akka.timeout", "300")
    conf.set("spark.shuffle.memoryFraction", "0.5")
    conf.set("spark.core.connection.ack.wait.timeout", "1800")

    sc = SparkContext(appName="hbase format",conf=conf)
    dic = sc.broadcast(sc.textFile("/data/develop/portrait/forbid.uid").map(lambda x:(int(x),None)).collectAsMap())
    rdd = sc.textFile("/data/develop/portrait/feedrecord_csv.dir.20151222/part-*")
    rdd1 = rdd.map(lambda x:filter_line(x,dic.value))\
            .filter(lambda x:x!=None)\
            .groupByKey()\
            .map(lambda (uid,linels):format(uid,linels))
    rdd1.saveAsTextFile("/data/develop/portrait/feedrecord_csv_hbase.dir.20151222")
    '''

    #预留后期扩展
    mission = '-info'
    inputpath = sys.argv[1]
    outputpath = sys.argv[2]
    if mission == "-info":
        sc = SparkContext(appName="gen hbase data "+inputpath)
        sc.textFile(inputpath)\
            .map(lambda x:format_info(x))\
            .filter(lambda x:x != None)\
            .saveAsTextFile(outputpath)
        sc.stop()
    '''
    elif mission == "itemidtitle":
        sc = SparkContext(appName="gen hbase data "+mission+' '+inputpath)
        sc.textFile(sys.argv[2])\
            .map(lambda x:format_relitemid(x))\
            .saveAsTextFile(outputpath)
        sc.stop()
    '''