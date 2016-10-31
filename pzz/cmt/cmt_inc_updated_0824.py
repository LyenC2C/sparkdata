#coding:utf-8
import sys, rapidjson, time
import rapidjson as json
from pyspark import SparkContext
from pyspark import SparkConf


def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\r", "")

def gen_uid_feedid(line):
    try:
        ls = line.strip().split("\001")
        return [ls[3], ls[2]]
    except:
        return None

def gen_uid_mark_feedid(x,y):
    dic = {1:"",2:""}
    for each in y:
        dic[each[0]] = each[1]
    if dic[1] == "":
        dic[1] = "0"
    return x+'\001'+dic[2]+'\001'+dic[1]

#feed指字段组成list的单条评论,cmt指字符连接的单条评论
#return [[[usermark,[feedid,feed]],[]],user_dic.keys]
def parse_cmt_v4(line_s):
    line = valid_jsontxt(line_s)
    ls = line.strip().split("\t")
    if len(ls) != 5:
        return None
    ts = ls[0]
    crawl_type=ls[2]
    json_txt = ls[4][11:-1]
    ob = json.loads(json_txt)
    itemid = ""
    if type(ob) == type({}) and ob.has_key("data") and ob["data"].has_key("rateList"):
        data = ob['data']
        feed_ls = []
        user_dic = {}
        for value in data['rateList']:
            try:
                l = []
                itemid = value.get('auctionNumId', '-')
                int(itemid)
                l.append(itemid)
                l.append(value.get('auctionTitle', '-').replace('\t', ''))
                feedid = value.get('id', '-')
                int(feedid)
                l.append(feedid)
                userid = value.get('userId', '-')
                usermark = value.get('userMark','')
                if len(usermark)!=24:
                    continue
                l.append(userid)
                # l.append(data.get('userStar'))
                feedback = value.get('feedback', '-').replace('\t', '')
                l.append(valid_jsontxt(feedback))
                date = value.get('feedbackDate', '-').replace(".","-")
                l.append(date)
                annoy = value.get('annoy', '-')
                l.append(annoy)
                l.append(ts)
                sku = json.dumps(value.get('skuMap'))
                l.append(sku)
                rate_type = value.get('rateType', '-')
                l.append(rate_type)
                l.append(crawl_type)
                l.append(usermark)
                #user
                user_nick = value.get('userNick', '-')
                l.append(user_nick)
                user_star = value.get('userStar', '-')
                key = userid+'\001'+usermark+'\001'+user_nick
                if user_dic.has_key(key):
                    pass
                else:
                    user_dic[key] = None
                #date
                date = date[:10].replace('-', '')
                int(date)
                if len(date) != 8:
                    print "date is wrong,now is "+date
                    continue
                # l.append(str(time.mktime(datetime.datetime.now().timetuple())))
                feed_ls.append([feedid, [usermark,l]])
            except Exception,e:
                print e,line
        #return [[[feedid,[usermark,"\001".join(l)]],[]],user_dic.keys]
        return [feed_ls,None,itemid]
    else :
        print 'not a json line',line_s.encode("utf-8")
    return None

def uniq_new_cmt(y):
    for each in y:
        return each

#将uid-feedidls更新为feedid-uid
def flat_feedid_uid_map(x,y):
    res = []
    for each in y:
        try:
            res.append([str(each),str(x)])
        except Exception,e:
            print e,each.encode("utf-8"),x.encode("utf-8")
    return res

#匹配mark与uid by feedid,并且区分出新旧数据
#return [1,[uid,mark]] 旧数据,匹配出uid与mark\
#   or  [2,[mark,cmt]] 新数据,匹配不到uid\
#   or  None    旧feedid
def match_feedid(x,y):
    dic = {1:None,2:None}
    for each in y:
        dic[each[0]] = each[1]
    #如果能对应上uid,说明feedid也存在,则返回其对应
    if dic[1] != None and dic[2] !=None:
        return [1,[dic[1], dic[2][0]]]
    #如果不存在历史feedid,返回新数据
    elif dic[1] == None and dic[2] != None:
        return [2,dic[2]]
    return None

#合并uidmark
def merge_uid_mark(x,y):
    dic = {"uid":str(x)}
    for each in y:
        for k in each:
            if k == "uid":
                continue
            if dic.has_key(k):
                dic[k] += each[k]
            else:
                dic[k] = each[k]
    return dic

def map_mark_uid(j):
    res = []
    uid = j["uid"]
    j.pop("uid")
    for k in j:
        res.append([k,uid])
    return res

#匹配mark到uid
def match_mark(x,y):
    dic = {1:None,2:None}
    uid = None
    res_feedls = []
    for each in y:
        dic[each[0]] = 1
        if each[0] == 1:
            uid = each[1]
    for each in y:
        if each[0] == 2:
            if uid != None:
                ls = each[1][1][:-1]
                ls[3] = uid
                res_feedls.append(ls)
            else:
                res_feedls.append(each[1][1][:-1])
    #能对应上uid
    if dic[1] != None and dic[2] != None:
        return [1,res_feedls]
    #有cmt 不能对应到uid
    elif dic[1] == None and dic[2] != None:
        return [2,res_feedls]
    else:
        return None

'''
def match_mark(x,y):
    dic = {1:None,2:None}
    for each in y:
        dic[each[0]] = each[1]
    try:
        #能对应上uid
        if dic[1] != None and dic[2] != None:
            ls = dic[2][1].split("\001")
            ls[3] = dic[1]
            return [1,ls,ls[0]]
        #有cmt 不能对应到uid
        elif dic[1] == None and dic[2] != None:
            ls = dic[2][1].split("\001")
            return [2,dic[2][1],ls[0]]
        else:
            return None
    except Exception,e:
        print e,dic[2]
'''

def merge_res_uid_feedids(x,y):
    res = x+'\001'+'0'
    resls = []
    for each in y:
        if type(each) == type([]):
            resls += each
        else:
            resls.append(each)
    if len(resls) > 0:
        res += '\001'
        res += '\001'.join(set(resls))
    return res

def cal_item_inc_num(x,y):
    dic = {1:0,2:0}
    for each in y:
        dic[each] += 1
    return x + '\t'+str(dic[1])+'\t'+str(dic[1]+dic[2])

def convert_uid_mark(x):
    j = json.loads(x)
    uid = j["uid"]
    j.pop("uid")
    res = []
    for k in j.keys():
        if k!="":
            res.append([k,uid])
    return res

def merge_mark_nouidcmt(x,y):
    dic = {"uid":None,"cmt":[]}
    dic_feedid = {}
    for each in y:
        if each[0] == 2:
            dic["uid"] = each[1]
        else:
            if dic_feedid.has_key(each[1]):
                pass
            else:
                dic["cmt"].append(each[2])
                dic_feedid[each[1]] = None
    if len(dic["cmt"]) == 0:
        return None
    if dic["uid"] != None:
        res = []
        for cmt in dic["cmt"]:
            cmt[3] = dic["uid"]
            res.append(cmt)
        return [1,res]
    else:
        return [0,dic["cmt"]]

def filter_cmt_by_feedids(x,y):
    feedid_dic = {}
    res_valid = []
    for each in y:
        if each[0] == 2:
            for feedid in each[1]:
                feedid_dic[feedid] = None
    for each in y:
        if each[0] == 1:
            if feedid_dic.has_key(each[1]):
                pass
            else:
                res_valid.append(each[2])
                feedid_dic[each[1]] = None
    return [res_valid,x+'\001'+'0'+'\001'+'\001'.join(feedid_dic.keys())]

def flat_falg_cmt(x,y):
    res = []
    for each in y:
        res.append([each[0],x])
    return res

if __name__ == "__main__":
    if sys.argv[1] == '-h':
        comment = '-gen_his_user_feedid \t argv[2]:output dir \t 提取库里uid feedid' + \
                  '-gen_data_inc \t argv[2]:his_item_feed_file argv[3]:data path to insert argv[4-5-6]:[all_feedid new_feedid data] output dir \t 根据his_item_feed_file,每日新增评论去重，过滤库里已有\n'

        print comment

    elif sys.argv[1] == '-gen_data_inc':
        uid_feedids = sys.argv[2]
        uid_mark = sys.argv[3]
        cmt_input_data = sys.argv[4]
        output_cmt_inc_data = sys.argv[5]
        output_cmt_inc_data_nouid = sys.argv[6]
        output_all_uid_feedids = sys.argv[7]
        output_all_uid_marks = sys.argv[8]
        output_item_inc_num = sys.argv[9]
        output_user = sys.argv[10]
        '''
        uid_feedids = "/data/develop/ec/tb/cmt/feedid/all_uid_mark_feedids.20160731"
        uid_mark = "/data/develop/ec/tb/cmt/uid_mark/uid_mark_freq.json.20160731"
        cmt_input_data = "/commit/comments/tmp/20160801/172.16.1.131_00155d3b5208.2016-08-01*"
        output_cmt_inc_data = "/data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.20160824.test"
        output_cmt_inc_data_nouid = "/data/develop/ec/tb/cmt/cmt_inc_data.nouid.20160824.test"
        output_all_uid_feedids = "/data/develop/ec/tb/cmt/all_uid_mark_feedids.20160824.test"
        output_all_uid_marks = "/data/develop/ec/tb/cmt/uid_mark_freq.json.20160824.test"
        output_item_inc_num = "/commit_feedbck/cmt/inc_item_num.20160824.test"
        output_user = "/data/develop/ec/tb/cmt/user/user.20160824.test"
        '''

        conf = SparkConf()
        conf.set("spark.network.timeout","2000s")
        conf.set("spark.akka.timeout","1000s")
        conf.set("spark.akka.frameSize","1000")
        sc = SparkContext(appName="gen_cmt_inc "+cmt_input_data,conf=conf)

        #历史uid-feedid [uid,feedidls]
        rdd_uid_feedids = sc.textFile(uid_feedids)\
            .map(lambda x:x.split("\001"))\
            .map(lambda x:[x[0],x[2:]])\
            .filter(lambda x:x[0] != "" and x[0] != '\\N')

        #历史feedid-uid [feedid,[1,uid]]
        rdd_feedid_uid = rdd_uid_feedids\
                .map(lambda (x,y):flat_feedid_uid_map(x,y))\
                .flatMap(lambda x:x)\
                .map(lambda x:[x[0],[1,x[1]]])

        #加载历史uid-mark [uid,[1,{"mark1":1,...}]]
        rdd_uid_mark = sc.textFile(uid_mark)\
                        .map(lambda x:json.loads(valid_jsontxt(x)))\
                        .map(lambda x:[x["uid"],x])

        #新采数据 [feed_ls,None,itemid],其中feed_ls=[[feedid,[mark,cmt]],...]
        rdd_new = sc.textFile(cmt_input_data)\
                    .filter(lambda x: 'SUCCESS' in x) \
                    .map(lambda x: parse_cmt_v4(x)) \
                    .filter(lambda x: x != None)

        #今日新采itemid, [itemid,[2,1]] 2表示计算位 1表示出现标识位
        rdd_new_item = rdd_new.map(lambda (x,y,z):z)\
                                        .filter(lambda x:x!="")\
                                        .map(lambda x:[x,[2,1]])

        #筛选出评论内容,并去重 [feedid,[2,feed]]
        rdd_new_cmt = rdd_new.map(lambda (x,y,z):x)\
                    .flatMap(lambda x:x)\
                    .groupByKey()\
                    .map(lambda (x,y):[x,[2,uniq_new_cmt(y)]])
        '''
                    .mapValues(list)\
                    .map(lambda (x,y):[x,[2,y[0]]])
        '''

        #用户uid-mark-nick
        rdd_new_user = rdd_new_cmt.map(lambda (x,y):y[1][1])\
                        .map(lambda x:[x[3]+'\001'+x[11]+'\001'+x[12],1])

        #保存uid-mark-nick
        rdd_new_user.groupByKey()\
                    .map(lambda (x,y):x)\
                    .coalesce(100)\
                    .saveAsTextFile(output_user)

        #匹配mark的uid,并且区分新旧数据
        rdd_feedid_match = rdd_new_cmt.union(rdd_feedid_uid)\
                    .groupByKey()\
                    .map(lambda (x,y):match_feedid(x,y))

        #清洗后新增评论数据
        #[mark,[2,feed]]
        rdd_new_cmt_clean_data = rdd_feedid_match\
                        .filter(lambda x:x != None and x[0] == 2)\
                        .map(lambda x:x[1])\
                        .map(lambda x:[x[0],[2,x]])

        #清洗后对应的uid和mark关系
        #return [uid,{uid,mark}]
        rdd_new_cmt_uid_mark = rdd_feedid_match\
                        .filter(lambda x:x != None and x[0] == 1)\
                        .map(lambda x:[x[1][0],json.loads('{\"uid\":"'+x[1][0]+'\","'+x[1][1]+'":'+'1}')])

        #合并的uid-mark,{"uid":"","mark1":1,...}
        rdd_uid_mark_merge = rdd_uid_mark.union(rdd_new_cmt_uid_mark)\
                    .groupByKey()\
                    .map(lambda (x,y):merge_uid_mark(x,y))

        #uid-mark 供下一步计算
        #[mark,[1,uid]]
        rdd_uid_mark_merge_map = rdd_uid_mark_merge\
                    .map(lambda x:map_mark_uid(x))\
                    .flatMap(lambda x:x)\
                    .map(lambda x:[x[0],[1,x[1]]])

        #return [1,feedls]:通过mark匹配上uid的
        #       [2,feedls]:没有匹配上uid的
        #       None:只有mark的,没有评论的
        rdd_cmt_inc = rdd_uid_mark_merge_map.union(rdd_new_cmt_clean_data)\
                    .groupByKey()\
                    .map(lambda (x,y):match_mark(x,y))\
                    .filter(lambda x:x!=None)


        #result: 新增评论存储数据
        rdd_cmt_inc_uid = rdd_cmt_inc.filter(lambda x:x[0] == 1)\
                    .map(lambda x:x[1])\
                    .flatMap(lambda x:x)
                    #.map(lambda x:'\001'.join(x[1]))

        rdd_cmt_inc_uid.map(lambda x:'\001'.join(x))\
                    .coalesce(400)\
                    .saveAsTextFile(output_cmt_inc_data)

        #result: 新增无uid评论数据
        #rdd_cmt_inc_nouid = \
        rdd_cmt_inc.filter(lambda x:x[0] == 2)\
                    .map(lambda x:x[1])\
                    .flatMap(lambda x:x)\
                    .map(lambda x:'\001'.join(x))\
                    .coalesce(200)\
                    .saveAsTextFile(output_cmt_inc_data_nouid)

        #result: 新uid-feedids
        #rdd_res_uid_feedids = \
        rdd_cmt_inc_uid.map(lambda x:[x[3],x[2]])\
                    .union(rdd_uid_feedids)\
                    .groupByKey()\
                    .map(lambda (x,y):merge_res_uid_feedids(x,y))\
                    .coalesce(300)\
                    .saveAsTextFile(output_all_uid_feedids)

        #result: uid_mark_freq:
        rdd_uid_mark_merge.map(lambda x:json.dumps(x))\
                        .coalesce(100)\
                        .saveAsTextFile(output_all_uid_marks)

        #reuslt:item inc num:
        rdd_item_inc_num = rdd_cmt_inc\
                        .map(lambda (x,y):flat_falg_cmt(x,y))\
                        .flatMap(lambda x:x)\
                        .groupByKey()\
                        .map(lambda (x,y):cal_item_inc_num(x,y))
        rdd_item_inc_num.coalesce(50)\
                        .saveAsTextFile(output_item_inc_num)

    elif sys.argv[1] == '-gen_data_add_nouid':
        input_nouid_data = sys.argv[2]
        input_uid_feedids = sys.argv[3]
        input_uid_mark = sys.argv[4]
        output_cmt_add = sys.argv[5]
        output_cmt_not_matched = sys.argv[6]
        output_uid_feedids = sys.argv[7]

        input_nouid_data = "/data/develop/ec/tb/cmt/tmpdata.nouid/cmt_inc_data.nouid.20160629"
        input_uid_feedids = "/data/develop/ec/tb/cmt/feedid/all_uid_mark_feedids.20160630"
        input_uid_mark = "/data/develop/ec/tb/cmt/uid_mark/uid_mark_freq.json.20160630"
        output_cmt_add = "/data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.20160101-20160630.add"
        output_cmt_not_matched = "/data/develop/ec/tb/cmt/tmpdata.nouid/cmt_inc_data.nouid.20160101-20160630.add"
        output_uid_feedids = "/data/develop/ec/tb/cmt/feedid/all_uid_mark_feedids.20160630.add"

        sc = SparkContext(appName="gen_nouid_add_cmt_inc "+input_nouid_data)

        #return [mark,[1,feedid,feed]]
        rdd_nouid = sc.textFile(input_nouid_data)\
                .map(lambda x:x.split("\001"))\
                .map(lambda x:[x[11],[1,x[2],x]])

        #return [mark,[2,uid]]
        rdd_mark_uid = sc.textFile(input_uid_mark)\
                        .map(lambda x:convert_uid_mark(x))\
                        .flatMap(lambda x:x)\
                        .map(lambda (x,y):[x,[2,y]])

        #return [1,cmtls] or [0,cmtls]
        rdd_nouidcmt_mark_union = rdd_nouid.union(rdd_mark_uid)\
                .groupByKey()\
                .map(lambda (x,y):merge_mark_nouidcmt(x,y))\
                .filter(lambda x:x!=None)

        #return [uid,[1,feedid,cmt]]
        rdd_cmt_uid_matched = rdd_nouidcmt_mark_union.filter(lambda x:x[0]==1)\
                .map(lambda x:x[1])\
                .flatMap(lambda x:x)\
                .map(lambda x:[x[3],[1,x[2],'\001'.join(x)]])

        #return [uid,[2,feedids]]
        rdd_uid_feedids = sc.textFile(input_uid_feedids)\
                .map(lambda x:x.split("\001"))\
                .map(lambda x:[x[0],[2,x[2:]]])

        #return [add_cmt_valid,uid+0+feedids]
        rdd_res = rdd_cmt_uid_matched.union(rdd_uid_feedids)\
                .groupByKey()\
                .map(lambda (x,y):filter_cmt_by_feedids(x,y))

        rdd_res.map(lambda (x,y):x)\
                .flatMap(lambda x:x)\
                .saveAsTextFile(output_cmt_add)

        rdd_res.map(lambda (x,y):y)\
                .saveAsTextFile(output_uid_feedids)

        #return [cmtls] no matched.
        rdd_cmt_uid_notmatched = rdd_nouidcmt_mark_union.filter(lambda x:x[0]==0)\
                .map(lambda x:x[1])\
                .flatMap(lambda x:x)\
                .map(lambda x:'\001'.join(x))\
                .saveAsTextFile(output_cmt_not_matched)







#pyspark --total-executor-cores  120 --executor-memory  10g --driver-memory 10g cmt_inc_baatch.py -gen_data_inc \
# /data/develop/ec/tb/cmt/feedid/all_uid_mark_feedids.20160316/part* /commit/comments/20160317/*  /data/develop/ec/tb/cmt/feedid/all_uid_mark_feedids.20160317 \
# /data/develop/ec/tb/cmt/feedid/inc_item_num.20160317 /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.20160317 /data/develop/ec/tb/cmt/user/user.20160317 \
# /data/develop/ec/tb/cmt/tmpdata.nouid/nouid_cmt_inc_data.20160317
