#coding:utf-8
from pyspark import  SparkContext
import sys

def filter_itemid(line,itemid_dic):
    ls = line.strip().split("\001")
    if itemid_dic.has_key(ls[0]):
        return line.strip()
    else:
        return None

def filter_userid(line,userid_dic):
    ls = line.strip().split("\001")
    if userid_dic.has_key(ls[2]):
        return line.strip()
    else:
        return None

if __name__ == '__main__':
    if sys.argv[1] == '-h':
        comment = '-extractbyitemid argv[2]:itemid input argv[3]:output dir 抽取feed历史by usertid \n'
        print comment

    elif sys.argv[1] == '-byitemid':
        sc = SparkContext(appName="extractbyitemid")
        itemid_dic = sc.broadcast(sc.textFile(sys.argv[2]).map(lambda x:[x.strip(),None]).collectAsMap())
        rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new/*")
        rdd.map(lambda x:filter_itemid(x,itemid_dic.value))\
            .filter(lambda x:x!=None)\
            .saveAsTextFile(sys.argv[3])

        sc.stop()

    elif sys.argv[1] == '-byuserid':
        sc = SparkContext(appName="extractbyuserid")
        userid_dic = sc.broadcast(sc.textFile(sys.argv[2]).map(lambda x:[x.strip(),None]).collectAsMap())
        rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new/*")
        rdd.map(lambda x:filter_userid(x,userid_dic.value))\
            .filter(lambda x:x!=None)\
            .saveAsTextFile(sys.argv[3])

        sc.stop()