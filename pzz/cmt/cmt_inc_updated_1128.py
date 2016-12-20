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
#return [itemid,feedls]
def parse_cmt_v5(line_s):
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
        item_feeds = []
        #user_dic = {}
        for value in data['rateList']:
            try:
                l = []
                itemid = value.get('auctionNumId', '-')
                int(itemid)
                if len(itemid) <=4:
                    continue
                l.append(itemid)
                l.append(value.get('auctionTitle', '-').replace('\t', ''))
                feedid = value.get('id', '-')
                int(feedid)
                if len(feedid) <=4:
                    continue
                l.append(feedid)
                userid = value.get('userId', '-')
                usermark = value.get('userMark','')
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
                usernick = value.get('userNick', '-')
                l.append(usernick)
                #
                #user_star = value.get('userStar', '-')
                #key = userid+'\001'+usermark+'\001'+user_nick
                #if user_dic.has_key(key):
                #    pass
                #else:
                #    user_dic[key] = None
                #
                #date
                date = date[:10].replace('-', '')
                int(date)
                if len(date) != 8:
                    print "date is wrong,now is "+date
                    continue
                # l.append(str(time.mktime(datetime.datetime.now().timetuple())))
                # l:[itemid,title,feedid,userid,feedback,date,annoy,ts,sku,rate_type,crawler_type,usermark,user_nick]
                item_feeds.append(l)
            except Exception,e:
                print e,line
        if itemid == "":
            return None
        return [itemid,item_feeds]
    else :
        print 'not a json line',line_s.encode("utf-8")
    return None


#新增评论数据按feedid去重
def uniq_new_feedid_mark_uid(x,y):
    for each in y:
        ls = each
        break
    return [ls[0],ls]

#
#def parse_item_feeduidls(x):
#    [itemid,feeds] = x.split("\001")
#    feedls = feeds.split("\002")
#    res = [item]

#返回mark_uid_new,其中 mark_uid_new=[[mark,uid],....]
def match_mark_uid_from_feed_in_item(x,y):
    feediduids = None
    for each in y:
        if type(each) != type([]):
            feediduids = each
    if feediduids == None:  #没有商品历史feedid 只有新的, mark不能关联到uid
        return None
    else: #有历史商品feed,构造dic:feedid-uid
        org_feedid_uid_dic = {}
        for each in feediduids.split("\002"):
            fid,uid = each.split("\003")
            org_feedid_uid_dic[fid] = uid
    mark_uid_dic = {}
    for each in y:
        if type(each) == type([]): #如果有新的feedid - mark
            for feed in each: #[itemid,feedid,uid,usermark,usernick]
                feedid = feed[1]
                mark = feed[3]
                if org_feedid_uid_dic.has_key(feedid): #旧的数据,用于mark 和 uid 对应
                    mark_uid_dic[mark] = org_feedid_uid_dic[feedid]
    mark_uid_new = []
    for mark in mark_uid_dic:
        mark_uid_new.append([mark,mark_uid_dic[mark]])
    return mark_uid_new

#返回新增cmt list
def match_new_cmt_from_feed_in_item(x,y):
    feediduids = None
    for each in y:
        if type(each) != type([]):
            feediduids = each
    org_feedid_uid_dic = {}
    if feediduids != None:  #根据历史商品评论构造feed dic
        for each in feediduids.split("\002"):
            fid,uid = each.split("\003")
            org_feedid_uid_dic[fid] = 1
    cmt_new = []
    for each in y:
        if type(each) == type([]): #如果有新的cmt
            for feed in each: #cmt
                feedid = feed[2]
                if org_feedid_uid_dic.has_key(feedid) == False: #旧的数据去重
                    cmt_new.append(feed)
    return cmt_new


#返回[[uid,mark],...]
def uid_mark_group(x,y):
    dic = {}
    for each in y:
        dic[each] = x
    res = []
    for k in dic:
        res.append([k,dic[k]])
    return res

#合并uidmark
def merge_uid_mark(x,y):
    #his: [uid,[1,{"uid":uid,mark1:n1,mark2:n2,....}]]
    #new: [uid,[2,list(mark)]]
    resdic = {"uid":str(x)}
    dic = {1:None,2:None}
    for each in y:
        dic[each[0]] = each[1]
    #分为三种情况
    if dic[1] != None and dic[2] != None:
        resdic = dic[1]
        resdic["latest"] = dic[2]
        for mark in dic[2]:
            if resdic.has_key(mark):
                resdic[mark] += 1
            else:
                resdic[mark] = 1
    elif dic[1] != None and dic[2] == None: #没有新增,沿用旧数据
        resdic = dic[1]
    elif dic[1] == None and dic[2] != None: #只有新增,构造
        resdic["latest"] = dic[2]
        for mark in dic[2]:
            resdic[mark] = 1
    return resdic

#转换uid-mark 为mark-uid
#return [mark,[1,uid]]
def trans_uid_mark(x):
    j = json.loads(x)
    uid = j["uid"]
    res = []
    if j.has_key("latest"):
        for mark in j["latest"]:
            res.append([mark,[1,uid]])
    else:
        j.pop("uid")
        for k in j:
            res.append([k,[1,uid]])
    return res

#return [0,0]:无cmt
#       [1,cmts]:匹配上uid的cmt
#       [2,cmts]:没有匹配上uid的cmt
def match_uid_by_mark(x,y):
    #[mark,[1,uid]]
    #[mark,[2,cmt]]
    dic = {1:None,2:[]}
    for each in y:
        if each[0] == 1:
            dic[1] = each[1]
        else:
            dic[2].append(each[1])
    if dic[1] == None : #没有匹配到uid
        return [2,dic[2]]
    elif dic[1] != None and len(dic[2]) == 0:   #有mark-uid 没有cmt
        return [0,0]
    else:#有mark-uid,有cmt
        res = []
        for each in dic[2]:
            ls = each
            ls[3] =  dic[1]
            res.append(ls)
        return [1,res]

#合并item下的历史feedid-uid 和新增评论的feedid-uid
#return format: itemid \001 feedid1 \003 uid1 \002 feedid2 \003 uid2 \002
def merge_item_feedid(x,y):
    # [itemid,[1,feediduidlsstr]]
    # [itemid,[2,[feedid,uid]],[2,[feedid,uid]],...]]
    dic = {1:None,2:[]}
    res_dic = {}
    for each in y:
        if each[0] == 1:
            dic[1] = each[1]
        else:
            dic[2].append(each[1])
    if dic[1] != None and len(dic[2]) != 0: #有旧的,有新的
        for each in dic[1].split("\002"):
            '''
            for feed in feeds.split("\003"):
                res_dic[feed[0]] = feed[1]
            '''
            fid,uid = each.split("\003")
            res_dic[fid] = uid
        for feed in dic[2]:
            res_dic[feed[0]] = feed[1]
        fiduidstrls = []
        for k in res_dic:
            fiduidstrls.append(k+'\003'+res_dic[k])
        res = x+'\001'+'\002'.join(fiduidstrls)
    elif dic[1] != None and len(dic[2]) == 0:#只有旧的
        res = x+'\001'+dic[1]
    else:#只有新的
        fiduidstrls = []
        for feed in dic[2]:
            fiduidstrls.append(feed[0]+'\003'+feed[1])
        res = x+'\001'+'\002'.join(fiduidstrls)
    return res




if __name__ == "__main__":
    if sys.argv[1] == '-h':
        comment = '-gen_his_user_feedid \t argv[2]:output dir \t 提取库里uid feedid' + \
                  '-gen_data_inc \t argv[2]:his_item_feed_file argv[3]:data path to insert argv[4-5-6]:[all_feedid new_feedid data] output dir \t 根据his_item_feed_file,每日新增评论去重，过滤库里已有\n'

        print comment

    #合并计算uid mark
    elif sys.argv[1] == '-merge_new_uid_mark_user':

        itemid_feediduid_ls = sys.argv[2]
        uid_mark = sys.argv[3]
        cmt_input_data = sys.argv[4]

        output_uid_mark = sys.argv[5]

        '''
        itemid_feediduid_ls = "/data/develop/ec/tb/cmt/itemid_feedid/itemid_feediduidls.20161030"
        uid_mark = "/data/develop/ec/tb/cmt/uid_mark/uid_mark_freq.json.20161031"
        cmt_input_data = "/commit/comments/201611*/*"
        output_uid_mark = "/data/develop/ec/tb/cmt/uid_mark/uid_mark_freq.json.20161129"
        output_new_cmt_uid_mark = ""
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
        conf.set("spark.default.parallelism","450")
        sc = SparkContext(appName="gen_cmt_inc "+cmt_input_data,conf=conf)

        #新采数据并去重,只用于处理mark uid: 返回[itemid,feedls],其中feed_ls=[[itemid,feedid,uid,usermark,usernick],...]
        rdd_new = sc.textFile(cmt_input_data)\
                    .filter(lambda x: 'SUCCESS' in x) \
                    .map(lambda x: parse_cmt_v5(x)) \
                    .filter(lambda x: x != None)\
                    .flatMap(lambda (x,y):y) \
                    .map(lambda x:[x[2],[x[0],x[2],x[3],x[11],x[12]]])\
                    .groupByKey() \
                    .map(lambda (x,y):uniq_new_feedid_mark_uid(x,y)) \
                    .groupByKey() \
                    .mapValues(list)

        #加载[itemid,feedid \003 uid \002 feedid \003 uid \002 ...]
        rdd_itemid_feediduid_ls= sc.textFile(itemid_feediduid_ls)\
                .map(lambda x:x.strip().split("\001"))

        #评论数据处理后的旧数据,用于筛选新的mark uid
        #mark_uid_match: [[mark,uid],....]
        rdd_mark_uid_match = rdd_new.union(rdd_itemid_feediduid_ls)\
                .groupByKey()\
                .map(lambda (x,y):match_mark_uid_from_feed_in_item(x,y))\
                .filter(lambda x:x!=None)\
                .flatMap(lambda x:x)\
                .groupByKey()

        #新匹配的uid-mark: # rdd_new_cmt_uid_mark: [[uid,{"uid":XX,mark:1}],...]
        rdd_cmt_uid_mark = rdd_mark_uid_match.map(lambda (x,y):uid_mark_group(x,y))\
                                        .flatMap(lambda x:x)\
                                        .groupByKey()\
                                        .mapValues(list)\
                                        .map(lambda (x,y):[x,list(set(y))])
        #保存新匹配uid mark
        rdd_cmt_uid_mark.map(lambda (x,y):x+'\001'+'\002'.join(list(set(y))))\
                        .saveAsTextFile(output_uid_mark)

        #构造uid mark #[uid,[2,list(mark)]]
        rdd_new_cmt_uid_mark = rdd_cmt_uid_mark.map(lambda (x,y):[x,[2,y]])


        #加载历史uid-mark [uid,[1,{"uid":XX,mark1:4,mark2:3,...}]]   1为标志位,用于区分新旧mark uid对应
        rdd_uid_mark = sc.textFile(uid_mark)\
                        .map(lambda x:json.loads(valid_jsontxt(x)))\
                        .map(lambda x:[x["uid"],[1,x]])

        #合并的uid-mark,{"uid":"","mark1":1,...}
        rdd_uid_mark_merge = rdd_uid_mark.union(rdd_new_cmt_uid_mark)\
                    .groupByKey()\
                    .map(lambda (x,y):merge_uid_mark(x,y))

        rdd_uid_mark_merge.map(lambda x:json.dumps(x))\
                    .coalesce(500)\
                    .saveAsTextFile(output_uid_mark)

    #计算新增cmt
    elif sys.argv[1] == '-gen_cmt_inc':

        itemid_feediduid_ls = sys.argv[2]
        uid_mark = sys.argv[3]
        cmt_input_data = sys.argv[4]

        output_itemid_feediduid_ls = sys.argv[5]
        output_cmt_inc_data = sys.argv[6]
        output_cmt_inc_data_nouid = sys.argv[7]
        output_user = sys.argv[8]

        #output_item_inc_num = sys.argv[9]

        '''
        itemid_feediduid_ls = "/data/develop/ec/tb/cmt/itemid_feedid/itemid_feediduidls.20161030"
        uid_mark = "/data/develop/ec/tb/cmt/uid_mark/uid_mark_freq.json.20161129"
        cmt_input_data = "/commit/comments/201611*/*"

        output_itemid_feediduid_ls = "/data/develop/ec/tb/cmt/itemid_feedid/itemid_feediduidls.20161129"
        output_cmt_inc_data = "/data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.20161129"
        output_cmt_inc_data_nouid = "/data/develop/ec/tb/cmt/tmpdata.nouid/cmt_inc_data.nouid.20161129"
        output_user = "/data/develop/ec/tb/cmt/user/user.20161129"

        output_item_inc_num = "/commit_feedbck/cmt/inc_item_num.20160824.test"

        '''

        conf = SparkConf()
        conf.set("spark.network.timeout","50000s")
        conf.set("spark.akka.timeout","50000s")
        conf.set("spark.akka.frameSize","1000")
        conf.set("spark.default.parallelism","5000")
        sc = SparkContext(appName="gen_cmt_inc "+cmt_input_data,conf=conf)

        #新采数据并去重,只用于处理mark uid: 返回[itemid,feedls],
        #其中feed_ls=一条cmt
        rdd_new = sc.textFile(cmt_input_data)\
                    .filter(lambda x: 'SUCCESS' in x) \
                    .map(lambda x: parse_cmt_v5(x)) \
                    .filter(lambda x: x != None)\
                    .flatMap(lambda (x,y):y) \
                    .map(lambda x:[x[2],x])\
                    .groupByKey() \
                    .map(lambda (x,y):uniq_new_feedid_mark_uid(x,y)) \
                    .groupByKey() \
                    .mapValues(list)

        #加载[itemid,feedid \003 uid \002 feedid \003 uid \002 ...]
        rdd_itemid_feediduid_ls= sc.textFile(itemid_feediduid_ls)\
                .map(lambda x:x.strip().split("\001"))

        #评论数据处理后的旧数据,用于筛选新的cmt
        ##清洗后新增评论数据: [cmt,....]
        rdd_cmt_new = rdd_new.union(rdd_itemid_feediduid_ls)\
                .groupByKey()\
                .map(lambda (x,y):match_new_cmt_from_feed_in_item(x,y))\
                .flatMap(lambda x:x)

        #保存uid-mark-nick
        rdd_cmt_new.map(lambda x:[x[3]+'\001'+x[11]+'\001'+x[12],1])\
                    .groupByKey()\
                    .map(lambda (x,y):x)\
                    .coalesce(100)\
                    .saveAsTextFile(output_user)


        #读取mark-uid
        rdd_mark_uid = sc.textFile(uid_mark)\
                    .map(lambda x:trans_uid_mark(x))\
                    .flatMap(lambda x:x)

        #合并mark-uid  与 cmt
        rdd_cmt_inc = rdd_cmt_new.map(lambda x:[x[11],[2,x[:-1]]])\
                    .union(rdd_mark_uid)\
                    .groupByKey()\
                    .map(lambda (x,y):match_uid_by_mark(x,y))

        #新增uid评论数据
        rdd_cmt_inc_uid = rdd_cmt_inc.filter(lambda (x,y):x==1)\
                                    .map(lambda (flag,cmts):cmts)\
                                    .flatMap(lambda x:x)
        rdd_cmt_inc_uid.map(lambda x:'\001'.join(x))\
                    .coalesce(400)\
                    .saveAsTextFile(output_cmt_inc_data)

        #新增无uid评论数据
        rdd_cmt_inc.filter(lambda (x,y):x==2)\
                    .map(lambda (flag,cmts):cmts)\
                    .flatMap(lambda x:x)\
                    .map(lambda x:'\001'.join(x))\
                    .coalesce(200)\
                    .saveAsTextFile(output_cmt_inc_data_nouid)

        #新item feed-uid
        #[itemid,title,feedid,userid,feedback,date,annoy,ts,sku,rate_type,crawler_type,usermark,user_nick]
        rdd_cmt_inc_uid.map(lambda x:[x[0],[2,[x[2],x[3]]]])\
                    .union(rdd_itemid_feediduid_ls.map(lambda (x,y):[x,[1,y]]))\
                    .groupByKey()\
                    .map(lambda (x,y):merge_item_feedid(x,y))\
                    .coalesce(500)\
                    .saveAsTextFile(output_itemid_feediduid_ls)


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
