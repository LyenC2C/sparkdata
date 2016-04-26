#coding:utf-8

import sys
import rapidjson as json
def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res
def f(x):
    s = x.strip().replace('\r\n','').replace("\u0000","").replace("\t"," ")
    j = json.loads(valid_jsontxt(s))
    if j.has_key("qun_id"):
        return j
    else:
        return None


def filter_group(x,y):
    ls = []
    if 1 in y:
        for each in y:
            if each != 1:
                ls.append([x]+each)
        return ls
    else:
        return None

def filter_qq_qqwb_group(x,y):
    if 1 in y:
        for each in y:
            if each != 1:
                return [x,each]
    else:
        return None

def filter_qq_base_group(x,y):
    if 1 in y:
        for each in y:
            if each != 1:
                return [x,each]
    else:
        return None

def pro_group(x,y):
    flag = 0
    qun_content = ""
    res = []
    for each in y:
        if each[0] == 2:
            flag = 1
            qun_content = each[1]
        if flag == 1:break
    for each in y:
        if each[0] == 1:
            res.append(each[1]+'\001'+qun_content)
    return res

def qqid_qun_group(x,y):
    res = ""
    for each in y:
        res += '#'.join(each[1:])
        res += '|'
    res = res[:-1] if res[-1] == '|' else res
    return [x,res]

def merge_group(x,y):
    dic  = {1:"",2:"",3:""}
    for each in y:
        dic[each[0]] = each[1]
    return x+'\001'+dic[1]+'\001'+dic[2]+'\001'+dic[3]

if __name__ == '__main__':
    from pyspark import SparkContext
    sc = SparkContext(appName="szty_user")

    #rdd_qid = sc.textFile("/user/yarn/service/szty/base_info.20160412.qid").map(lambda x:(x.strip(),1))
    #改为通用
    input_qqid = sys.argv[1]
    output_path = sys.argv[2]
    rdd_qid = sc.textFile(input_qqid).map(lambda x:(x.strip(),1))

    #filtered  [qunid,[1,qqid+'\001'+name]]
    rdd1 = sc.textFile("/data/develop/qq/group_member.json")\
            .map(lambda x:f(x))\
            .filter(lambda x:x!=None)\
            .map(lambda j:[str(j["qq_id"]),[str(j["qun_id"]),j["name"]]])\
            .union(rdd_qid)\
            .groupByKey()\
            .mapValues(list)\
            .map(lambda (x,y):filter_group(x,y))\
            .filter(lambda x:x!=None)\
            .flatMap(lambda x:x)\
            .map(lambda x:[x[1],[1,x[0]+'\001'+x[2].decode("utf-8")]])

    #[qunid,[2,qun_id+'\001'+title+'\001'+qun_text]]
    rdd2 = sc.textFile("/data/develop/qq/qun_info.tsv/*")\
            .map(lambda x:x.split("\t"))\
            .map(lambda x:[x[0],[2,x[0]+'\001'+x[1]+'\001'+x[2]]])

    #str(j["qq_id"])+'\001'+j["name"]+'\001'+j["qun_id"]+'\001'+j["title"]+'\001'+j["qun_text"]
    rdd3 = rdd1.union(rdd2).groupByKey()\
            .map(lambda (x,y):pro_group(x,y))\
            .flatMap(lambda x:x)\
            .map(lambda x:x.replace("|","").replace("#",""))\
            .map(lambda x:x.split("\001"))\
            .map(lambda x:[x[0],x])\
            .groupByKey()\
            .map(lambda (x,y):qqid_qun_group(x,y))\
            .map(lambda (x,y):[x,[3,y]])

    '''原来是base已经挑好
    rdd_base = sc.textFile("/user/yarn/service/szty/base_info.20160412/*")\
                .map(lambda x:x.split("\001"))\
                .map(lambda x:[x[0],[1,'\001'.join(x[1:])]])
    '''
    #现修改为通用
    rdd_base = sc.textFile("/user/yarn/qq/base_info.dir/part*")\
                .map(lambda x:x.split("\001"))\
                .map(lambda x:(x[0],'\001'.join(x[1:])))\
                .union(rdd_qid)\
                .groupByKey()\
                .mapValues(list)\
                .map(lambda (x,y):filter_qq_base_group(x,y))\
                .filter(lambda x:x!=None)\
                .map(lambda (x,y):[x,[1,y]])

    rdd_qqwb = sc.textFile("/user/yarn/qqwb/qq-qqwb.all.till20160328")\
                .map(lambda x:x.split("\t"))\
                .map(lambda x:[x[0],x[1]])\
                .union(rdd_qid)\
                .groupByKey()\
                .mapValues(list)\
                .map(lambda (x,y):filter_qq_qqwb_group(x,y))\
                .filter(lambda x:x!=None)\
                .map(lambda (x,y):[x,[2,y]])

    rdd3.union(rdd_base).union(rdd_qqwb)\
            .groupByKey()\
            .map(lambda (x,y):merge_group(x,y))\
            .saveAsTextFile(output_path)


