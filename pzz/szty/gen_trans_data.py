#coding:utf-8


import rapidjson as json
def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res
def f(x):
    s = x.strip().replace('\r\n','').replace("\u0000","").replace("\t"," ")
    return json.loads(valid_jsontxt(s))

def filter_qq(x,qid_dic):
    j = f(x)
    if qid_dic.has_key(str(j["qq_id"])):
        return [str(j["qun_id"]),[1,str(j["qq_id"])+'\001'+j["name"]]]
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
    if flag == 1:
        for each in y:
            if each[0] == 1:
                res.append(each[1]+'\001'+qun_content)
    return res

if __name__ == '__main__':
    from pyspark import SparkContext
    sc = SparkContext(appName="xzx_iteminfo_genbase")
    qid_dic = sc.broadcast(sc.textFile("/user/yarn/service/szty/base_info.20160412.qid").map(lambda x:(x.strip(),None)).collectAsMap())

    rdd1 = sc.textFile("/data/develop/qq/group_member.json")\
            .map(lambda x:f(x))\
            .filter(lambda x:x!=None)
    rdd2 = sc.textFile("/data/develop/qq/qun_info.tsv/*")\
            .map(lambda x:x.split("\t"))\
            .map(lambda x:[x[0],[2,x[0]+'\001'+x[1]+'\001'+x[2]]])

    rdd1.union(rdd2).groupByKey()\
            .map(lambda x:pro_group(x,y))\
            .flatMap(lambda x:x)\
            .saveAsTextFile("/user/yarn/service/szty/qunpart.20160412")