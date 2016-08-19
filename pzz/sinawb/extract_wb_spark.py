#coding:utf-8
import sys
import json
import rapidjson as json
from pyspark import SparkContext

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\001", "").replace("\n", " ")

def filter_fri_by_uid(line,dic):
    j = json.loads(valid_jsontxt(line.strip()))
    if type(j) != type({}):
        return None
    uid = j["uid"]
    if dic.has_key(uid):
        #j.pop("_id")
        if j.has_key("total_number"):
            j.pop("total_number")
        return line.strip()
    else:
        return None

def filter_flw_by_uid(line,idls):
    res = []
    j = json.loads(valid_jsontxt(line.strip()))
    if type(j) == type({}):
        uid = j["uid"]
        for wbid in idls:
            if j.has_key('ids') and wbid in j["ids"]:
                res.append([wbid,uid])
    return res

def filter_info_by_uid(line,iddic):
    j = json.loads(valid_jsontxt(line.strip()))
    uid = j["idstr"]
    if iddic.has_key(uid):
        return line.strip()
    else:
        return None

def f(x):
    j = json.loads(valid_jsontxt(x.strip()))
    if j['idstr'] != '':
        return [j['idstr'],x.strip()]
    else:
        return None

def map_tag_big(x):
    try:
        j = json.loads(valid_jsontxt(x.strip()))
        if j['uid'] != '':
            return [str(j['uid']),[2,x.strip()]]
    except:
        return None

def map_fri_big(x):
    try:
        j = json.loads(valid_jsontxt(x.strip()))
        if j['id'] != '':
            return [str(j['id']),[2,x.strip()]]
    except:
        return None

def filter_use_dic(x,y):
    dic = {1:None,2:None}
    for each in y:
        dic[each[0]] = each[1]
    if dic[1] !=None and dic[2]!=None:
        return dic[2]

def match(x,y):
    flag = 0
    for each in y:
        if each == 1:
            flag = 1
    if flag == 1:
        for each in y:
            if each != 1:
                return each
    return None

if __name__ == '__main__':
    if sys.argv[1] == '-h':
        comment = '-filter_fri_by_uid 抽取fri argv[2]:target id input argv[3]:output dir \n'
        print comment
        print 'args: idfile outdir'

    if sys.argv[1] == '-filter_fri_by_uid':
        sc = SparkContext(appName="filter fri by uid")
        tel_map  = sc.broadcast(sc.textFile(sys.argv[2]).map(lambda x:(int(x.strip()),None)).collectAsMap())
        rdd_f = sc.textFile("/user/yarn/weibo/rel_fri.json.20160401")
        rdd_f.map(lambda x:filter_fri_by_uid(x,tel_map.value))\
                .filter(lambda x:x!=None)\
                .saveAsTextFile(sys.argv[3])
        sc.stop()

    elif sys.argv[1] == '-filter_fri_by_big_uid':
        sc = SparkContext(appName="filter fri by big uid")
        rdd_uid  = sc.textFile(sys.argv[2]).map(lambda x:[x.strip(),[1,1]])
        rdd_uinfo = sc.textFile("/data/develop/sinawb/rel_fri.json.20160401")\
                    .map(lambda x:map_tag_big(x))\
                    .filter(lambda x:x!=None)

        rdd_uid.union(rdd_uinfo)\
                .groupByKey()\
                .map(lambda (x,y):filter_use_dic(x,y))\
                .filter(lambda x:x!=None)\
                .saveAsTextFile(sys.argv[3])
        sc.stop()


    elif sys.argv[1] == '-filter_flw_by_uid':
        sc = SparkContext(appName="filter flw by uid")
        id_ls = sc.broadcast(sc.textFile(sys.argv[2]).map(lambda x:int(x.strip())).collect())
        rdd_f = sc.textFile("/user/yarn/weibo/rel_fri.json.20160401")
        rdd_f.map(lambda x:filter_flw_by_uid(x,id_ls.value))\
                .filter(lambda x:len(x)>0)\
                .flatMap(lambda x:x)\
                .map(lambda (x,y):str(x)+'\t'+str(y))\
                .saveAsTextFile(sys.argv[3])
        sc.stop()

    elif sys.argv[1] == '-filter_info_by_big_uid':
        sc = SparkContext(appName="filter info by big uid")
        rdd_uid  = sc.textFile(sys.argv[2]).map(lambda x:[x.strip(),1])
        rdd_uinfo = sc.textFile("/user/yarn/weibo/user_info.json.20160401")\
                    .map(lambda x:f(x))\
                    .filter(lambda x:x!=None)

        rdd_uid.union(rdd_uinfo)\
                .groupByKey()\
                .map(lambda (x,y):match(x,y))\
                .filter(lambda x:x!=None)\
                .saveAsTextFile(sys.argv[3])

        sc.stop()

    elif sys.argv[1] == '-filter_info_by_uid':
        sc = SparkContext(appName="filter info by uid")
        id_dic = sc.broadcast(sc.textFile(sys.argv[2]).map(lambda x:int(x.stip()),1).collectAsMap())
        rdd_f = sc.textFile("/user/yarn/weibo/user_info.json.20160401")
        rdd_f.map(lambda x:filter_info_by_uid(x,id_dic.value))\
                .filter(lambda x:x!=None)\
                .saveAsTextFile(sys.argv[3])
        sc.stop()

    elif sys.argv[1] == '-filter_tag_by_big_uid':
        sc = SparkContext(appName="filter tag by big uid")
        rdd_uid  = sc.textFile(sys.argv[2]).map(lambda x:[x.strip(),[1,1]])
        rdd_uinfo = sc.textFile("/user/yarn/weibo/tag/*")\
                    .map(lambda x:map_tag_big(x))\
                    .filter(lambda x:x!=None)

        rdd_uid.union(rdd_uinfo)\
                .groupByKey()\
                .map(lambda (x,y):filter_use_dic(x,y))\
                .filter(lambda x:x!=None)\
                .saveAsTextFile(sys.argv[3])

        sc.stop()



