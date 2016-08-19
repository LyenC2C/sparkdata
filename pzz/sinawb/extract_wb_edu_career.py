#coding:utf-8
import sys
import rapidjson as rjson
from pyspark import  SparkContext
import json

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\001", "").replace("\n", " ")


def map_tel(x,tel_map):
    ls = x.strip().split("\001")
    uid = ls[2]
    tel = tel_map[uid]
    return [tel_map[uid],[tel]+ls]

def filter_info_by_uid(x,y):
    if 1 in y:
        for each in y:
            if each != 1:
                return [x,each]
    else:
        return None

if __name__ == '__main__':
    '''
    ls = []
    tel= "11"
    for line in sys.stdin:
        ls.append(line.strip())
    print cal(tel,ls)
    '''
    if sys.argv[1] == '-career':
        sc = SparkContext(appName="get admaster weibo")
        inputpath = sys.argv[2]
        outputpath = sys.argv[3]
        rdd_wbid = sc.textFile(inputpath).map(lambda x:(int(x.strip()),1))
        rdd_data = sc.textFile("weibo/career/user_career.json_17*")\
                .map(lambda x:rjson.loads(valid_jsontxt(x)))\
                .flatMap(lambda x:x)\
                .filter(lambda x:x["career"]!=[])\
                .map(lambda x:[x["id"],rjson.dumps(x)])

        rdd_data.union(rdd_wbid)\
                .groupByKey()\
                .mapValues(list)\
                .map(lambda (x,y):filter_info_by_uid(x,y))\
                .filter(lambda x:x!=None)\
                .map(lambda (x,y):json.dumps(json.loads(y),ensure_ascii=False).encode("utf-8"))\
                .saveAsTextFile(outputpath)
        sc.stop()
    elif sys.argv[1] == '-edu':
        sc = SparkContext(appName="get weibo edu")
        inputpath = sys.argv[2]
        outputpath = sys.argv[3]
        rdd_wbid = sc.textFile(inputpath).map(lambda x:(int(x.strip()),1))
        rdd_data = sc.textFile("weibo/edu/useredu.json_17*")\
                .map(lambda x:rjson.loads(valid_jsontxt(x)))\
                .flatMap(lambda x:x)\
                .filter(lambda x:x["education"]!=[])\
                .map(lambda x:[x["id"],rjson.dumps(x)])

        rdd_data.union(rdd_wbid)\
                .groupByKey()\
                .mapValues(list)\
                .map(lambda (x,y):filter_info_by_uid(x,y))\
                .filter(lambda x:x!=None)\
                .map(lambda (x,y):json.dumps(json.loads(y),ensure_ascii=False).encode("utf-8"))\
                .saveAsTextFile(outputpath)
        sc.stop()
'''
    tel_dic = sc.broadcast(sc.textFile(sys.argv[1]).map(lambda x:rjson.loads(valid_jsontxt(x))).map(lambda x:[x["TB"],x["tel"]]).collectAsMap())
    rdd = sc.textFile(sys.argv[2])
    rdd.map(lambda x:map_tel(x,tel_dic.value))\
        .groupByKey()\
        .map(lambda (x,y):cal(x,y))\
        .saveAsTextFile(sys.argv[3])
'''

