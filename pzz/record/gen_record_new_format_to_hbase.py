#coding:utf-8
import sys
from pyspark import *

#0		item_id	bigint
#1		feed_id	string
#2		user_id	string
#3		content_length	int
#4		annoy	string
#5		ds	string
#6		datediff	int
#7		sku	string
#8		title	string
#9		cat_id	string
#10		root_cat_id	string
#11		root_cat_name	string
#12		brand_id	string
#13		brand_name	string
#14		bc_type	string
#15		price	string
#16		shop_id	string
#17		location	string

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
    ls = line.replace(",","").replace("|"," ").split("\001")
    try:
        int(ls[2])
    except:
        return None
    if len(ls[2]) <4 or len(ls[2]) > 15 or ls[14] == '\\N':
        return None
    #rowkey:itemid+_+60101+0000
    #rowkey = ls[2]+'_'+ls[5][-5:]+ls[1][-4:]
    #修改为   99999-ls[5][-5:]
    rowkey = ls[2]+'_'+str(99999-int(ls[5][-5:]))+ls[1][-4:]
    #[ds,catid,root_cat_id,brand_name,price,bc_type]
    v_info = '|'.join([ls[5],ls[9],ls[10],ls[13],ls[15],ls[14]])
    v_item = '|'.join([ls[0],ls[17],ls[8],ls[7]])
    return rowkey+','+v_info+','+v_item

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
    inputpath = sys.argv[1]
    outputpath = sys.argv[2]
    if mission == "info":
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