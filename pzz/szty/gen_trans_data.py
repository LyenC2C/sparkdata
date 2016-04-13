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

def filter_group_from_qq(x,y):


def filter_qq(x,qid_dic):
    j = f(x)
    if qid_dic.has_key(str(j["qq_id"])):
        return [str(j["qun_id"]),[1,str(j["qq_id"])+'\001'+j["name"]]]
    else:
        return None

def filter_group(x,y):
    if 1 in y:
        data = y
        data.remove(1)
        return data[0]
    else:
        return None

def filter_wb_qq(x,qid_dic):
    ls = x.strip().split("\t")
    if qid_dic.has_key(ls[0]):
        return [ls[0],ls[1]]
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
        res += '|'.join(each[1:])
        res += '||'
    res = res[:-2] if res[-2:] == '||' else res
    return [x,res]

def merge_group(x,y):
    dic  = {1:"",2:"",3:""}
    for each in y:
        dic[each[0]] = each[1]
    return x+'\001'+dic[1]+'\001'+dic[2]+'\001'+dic[3]

if __name__ == '__main__':
    from pyspark import SparkContext
    sc = SparkContext(appName="xzx_iteminfo_genbase")
    rdd_qid = sc.textFile("/user/yarn/service/szty/base_info.20160412.qid").map(lambda x:(x.strip(),1))

    #filtered  [qunid,[1,qqid+'\001'+name]]
    rdd1 = sc.textFile("/data/develop/qq/group_member.json")\
            .map(lambda x:f(x))\
            .map(lambda j:[str(j["qq_id"]),[str(j["qun_id"]),j["name"]]])\
            .union(rdd_qid)\
            .groupByKey()\
            .mapValues(list)\
            .map(lambda (x,y):filter_group(x,y))\
            .filter(lambda x:x!=None)\
            .map(lambda (x,y):[y[0],[1,x+'\001'+y[1]]])

    #[qunid,[2,qun_id+'\001'+title+'\001'+qun_text]]
    rdd2 = sc.textFile("/data/develop/qq/qun_info.tsv/*")\
            .map(lambda x:x.split("\t"))\
            .map(lambda x:[x[0],[2,x[0]+'\001'+x[1]+'\001'+x[2]]])

    #str(j["qq_id"])+'\001'+j["name"]+'\001'+j["qun_id"]+'\001'+j["title"]+'\001'+j["qun_text"]
    rdd3 = rdd1.union(rdd2).groupByKey()\
            .map(lambda x:pro_group(x,y))\
            .flatMap(lambda x:x)\
            .map(lambda x:x.replace("|",""))\
            .map(lambda x:x.split("\001"))\
            .map(lambda x:[x[0],x])\
            .groupByKey()\
            .map(lambda (x,y):qqid_qun_group(x,y))\
            .map(lambda x:[3,'\001'.join(x[1:])])

    rdd_base = sc.textFile("/user/yarn/service/szty/base_info.20160412/*")\
                .map(lambda x:x.split("\001"))\
                .map(lambda x:[x[0],[1,'\001'.join(x[1:])]])

    rdd_qqwb = sc.textFile("/user/yarn/qqwb/qq-qqwb.all.till20160328")\
                .map(lambda x:x.split("\t"))\
                .map(lambda x:[x[0],x[1]])\
                .union(rdd_qid)\
                .groupByKey()\
                .mapValues(list)\
                .map(lambda (x,y):filter_group(x,y))\
                .filter(lambda x:x!=None)\
                .map(lambda x:[x[0],[2,x[1]]])

    rdd3.union(rdd_base).union(rdd_qqwb)\
            .groupByKey()\
            .map(lambda (x,y):merge_group(x,y))\
            .saveAsTextFile("service/szty/result.20160412")


