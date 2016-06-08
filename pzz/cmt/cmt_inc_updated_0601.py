#coding:utf-8
import sys, rapidjson, time
import rapidjson as json
from pyspark import SparkContext


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


#return [[[usermark,[feedid,"\001".join(l)]],[]],user_dic.keys]
def parse_cmt_v4(line_s):
    line = valid_jsontxt(line_s)
    ls = line.strip().split("\t")
    if len(ls) != 6:
        return None
    ts = ls[0]
    crawl_type=ls[2]
    json_txt = ls[5][11:-1]
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
                user_star = value.get('userStar', '-')
                key = userid+'\001'+usermark+'\001'+user_nick+'\001'+user_star
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
                feed_ls.append([feedid, [usermark,"\001".join(l)]])
            except Exception,e:
                print e,line
        #return [[[feedid,[usermark,"\001".join(l)]],[]],user_dic.keys]
        return [feed_ls,None,itemid]
    else :
        print 'not a json line',line_s.encode("utf-8")
    return None

#将uid-feedidls更新为feedid-uid
def flat_feedid_uid_map(x,y):
    res = []
    for each in y:
        res.append([each,x])
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
    for each in y:
        dic[each[0]] = each[1]
    try:
        if dic[1] != None and dic[2] != None:
            ls = dic[2][1].split("\001")
            ls[3] = dic[1]
            return [1,ls,ls[0]]
        elif dic[1] == None and dic[2] != None:
            ls = dic[2][1].split("\001")
            return [2,dic[2][1],ls[0]]
        else:
            return None
    except Exception,e:
        print e,dic[2]

def merge_res_uid_feedids(x,y):
    res = x+'\001'+'0'
    resls = []
    for each in y:
        if type(each) == type([]):
            resls += each
        else:
            resls.append(each)
    if len(resls) > 0:
        res += '\001'.join(set(resls))
    return res

def cal_item_inc_num(x,y):
    dic = {1:0,2:0}
    for each in y:
        dic[each] += 1
    return x + '\t'+str(dic[1])+'\t'+str(dic[1]+dic[2])


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
        '''
        uid_feedids = "/data/develop/ec/tb/cmt/feedid/all_uid_mark_feedids.20160530"
        uid_mark = "/data/develop/ec/tb/cmt/uid_mark_freq.json.20160530"
        cmt_input_data = "/commit/comments/tmp/*/*"
        output_cmt_inc_data = "/data/develop/ec/tb/cmt/cmt_inc_data.uid.20160603"
        output_cmt_inc_data_nouid = "/data/develop/ec/tb/cmt/cmt_inc_data.nouid.20160603"
        output_all_uid_feedids = "/data/develop/ec/tb/cmt/feedid/all_uid_mark_feedids.20160603"
        output_all_uid_marks = "/data/develop/ec/tb/cmt/uid_mark_freq.json.20160603"
        output_item_inc_num = "/commit_feedbck/cmt/inc_item_num.20160603"
        '''

        sc = SparkContext(appName="gen_cmt_inc "+cmt_input_data)

        #历史uid-feedid [uid,feedidls]
        rdd_uid_feedids = sc.textFile(uid_feedids)\
            .map(lambda x:x.split("\001"))\
            .map(lambda x:[x[0],x[2:]])

        #历史feedid-uid [feedid,[1,uid]]
        rdd_feedid_uid = rdd_uid_feedids\
                .map(lambda (x,y):flat_feedid_uid_map(x,y))\
                .flatMap(lambda x:x)\
                .map(lambda x:[x[0],[1,x[1]]])

        #加载历史uid-mark [uid,[1,{"mark1":1,...}]]
        rdd_uid_mark = sc.textFile(uid_mark)\
                        .map(lambda x:json.loads(valid_jsontxt(x)))\
                        .map(lambda x:[x["uid"],x])

        #新采数据 [[feedid,[mark,cmt],...],None,itemid]
        rdd_new = sc.textFile(cmt_input_data)\
                    .filter(lambda x: 'SUCCESS' in x) \
                    .map(lambda x: parse_cmt_v4(x)) \
                    .filter(lambda x: x != None)

        #今日新采itemid, [x,[2,1]] 2表示计算位 1表示出现标识位
        rdd_new_item = rdd_new.map(lambda (x,y,z):z)\
                                        .filter(lambda x:x!="")\
                                        .map(lambda x:[x,[2,1]])

        #筛选出评论内容,并去重 [feedid,[2,cmt]]
        rdd_new_cmt = rdd_new.map(lambda (x,y,z):x)\
                    .flatMap(lambda x:x)\
                    .groupByKey()\
                    .mapValues(list)\
                    .map(lambda (x,y):[x,[2,y[0]]])

        #匹配mark的uid,并且区分新旧数据
        rdd_feedid_match = rdd_new_cmt.union(rdd_feedid_uid)\
                    .groupByKey()\
                    .map(lambda (x,y):match_feedid(x,y))

        #清洗后新增评论数据
        #[mark,[2,cmt]]
        rdd_new_cmt_clean_data = rdd_feedid_match\
                        .filter(lambda x:x != None and x[0] == 2)\
                        .map(lambda x:x[1])\
                        .map(lambda x:[x[0],[2,x]])

        #清洗后对应的uid和mark关系
        rdd_new_cmt_uid_mark = rdd_feedid_match\
                        .filter(lambda x:x != None and x[0] == 1)\
                        .map(lambda x:[x[1][0],{"uid":x[1][0],x[1][1]:1}])

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

        rdd_cmt_inc = rdd_uid_mark_merge_map.union(rdd_new_cmt_clean_data)\
                    .groupByKey()\
                    .map(lambda (x,y):match_mark(x,y))\
                    .filter(lambda x:x!=None)


        #result: 新增评论存储数据
        rdd_cmt_inc_uid = rdd_cmt_inc.filter(lambda x:x[0] == 1)\
                    .map(lambda x:x[1])
                    #.map(lambda x:'\001'.join(x[1]))

        rdd_cmt_inc_uid.map(lambda x:'\001'.join(x))\
                    .coalesce(200)\
                    .saveAsTextFile(output_cmt_inc_data)

        #result: 新增无uid评论数据
        rdd_cmt_inc_nouid = rdd_cmt_inc.filter(lambda x:x[0] == 2)\
                    .map(lambda x:x[1])\
                    .coalesce(200)\
                    .saveAsTextFile(output_cmt_inc_data_nouid)

        #result: 新uid-feedids
        rdd_res_uid_feedids = rdd_cmt_inc_uid.map(lambda x:[x[3],x[2]])\
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
        rdd_item_inc_num = rdd_cmt_inc.map(lambda (x,y,z):[y,x])\
                        .groupByKey()\
                        .map(lambda (x,y):cal_item_inc_num(x,y))
        rdd_item_inc_num.saveAsTextFile(output_item_inc_num)




#pyspark --total-executor-cores  120 --executor-memory  10g --driver-memory 10g cmt_inc_baatch.py -gen_data_inc \
# /data/develop/ec/tb/cmt/feedid/all_uid_mark_feedids.20160316/part* /commit/comments/20160317/*  /data/develop/ec/tb/cmt/feedid/all_uid_mark_feedids.20160317 \
# /data/develop/ec/tb/cmt/feedid/inc_item_num.20160317 /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.20160317 /data/develop/ec/tb/cmt/user/user.20160317 \
# /data/develop/ec/tb/cmt/tmpdata.nouid/nouid_cmt_inc_data.20160317
