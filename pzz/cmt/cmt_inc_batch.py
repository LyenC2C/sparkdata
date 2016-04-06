#coding:utf-8
import sys, rapidjson, time
import rapidjson as json
from pyspark import SparkContext


def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")

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


'''
      {
        "id": "259696832339",
        "auctionNumId": "8618812545",
        "userId": "2699367733",
        "userNick": "亚**8",
        "userStar": "2",
        "userStarPic": "//img.alicdn.com/newrank/b_red_2.gif",
        "headPicUrl": "//img.alicdn.com/tps/i3/TB1yeWeIFXXXXX5XFXXuAZJYXXX-210-210.png_40x40.jpg",
        "annoy": "1",
        "rateType": "1",
        "feedback": "给个好评，但是物流不给力。",
        "feedbackDate": "2015-12-08",
        "reply": "",
        "skuMap": {
          "食品口味": "牛奶味",
          "剩余保质期": "6个月以上"
        },
        "hasDetail": "false",
        "allowInteract": "false",
        "userMark": "WMAah0h1zjBN/NF/Lb+B/Q==",
        "share": {
          "shareSupport": "false",
          "shareURL": "//h5.m.taobao.com/user_comment/comment_detail.html?rateId=259696832339&sellerId=MMGlevH8Yv0ZhMC*ev8lePmxyPkZhOmkGMmPzPF-IP0gT&isEncode=true"
        }
      }
'''
#return [[[usermark,[feedid,"\001".join(l)]],[]],user_dic.keys]
def parse_cmt_v3(line_s):
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
                if usermark == "":
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
                feed_ls.append([usermark, [feedid,"\001".join(l)]])
            except Exception,e:
                print e,line
        #return [[[usermark,[feedid,"\001".join(l)]],[]],user_dic.keys]
        return [feed_ls,user_dic.keys(),itemid]
    return None


def uniq_cmt(ls):
    rls = []
    feedid_dic = {}
    for one in ls:
        feedid = one[0]
        if feedid_dic.has_key(feedid) == False:
            rls.append(one)
            feedid_dic[feedid] = None
    return rls

def clean_data_by_his_feedid(itemid,y):
    feedid_dic = {}
    #返回数据ls
    rls = []
    #返回所有评论id ls
    all_feed_ls = [itemid]
    #返回新增评论id ls
    new_item_feedid_ls = [itemid]
    today_flag = "0"
    for flag,feeds in y:
        if flag == 0:
            for feedid in feeds:
                feedid_dic[feedid] = None
    for flag,feeds in y:
        if flag == 1:
            today_flag = "1"
            for feedid,feeddata in feeds:
                if feedid_dic.has_key(feedid) == False:
                    rls.append(feeddata)
                    new_item_feedid_ls.append(feedid)
                    feedid_dic[feedid] = None

    for k in feedid_dic.keys():
        all_feed_ls.append(k)

    return [rls,all_feed_ls,new_item_feedid_ls,today_flag]

def clean_data_by_his_mark_feedid(usermark,y):
    feedid_dic = {}
    #返回有uid数据ls
    existuid_rls = []
    #返回无uid数据ls
    nouid_rls = []
    uid = '0'
    exist_uid_flag = 0

    for each in y:
        if each[0] == 0:
            uid = each[1]
            exist_uid_flag = 1
            for feedid in each[2]:
                feedid_dic[feedid] = None

    for each in y:
        if each[0] == 1:
            if exist_uid_flag == 1:
                for feedid,feeddata in each[1]:
                    if feedid_dic.has_key(feedid) == False:
                        ls = feeddata.split("\001")
                        if ls[3] == '0':
                            ls[3] = str(uid)
                        #print feeddata,ls,uid
                        existuid_rls.append('\001'.join(ls))
                        #new_user_feedid_ls.append(feedid)
                        feedid_dic[feedid] = None
            else:
                for feedid,feeddata in each[1]:
                    nouid_rls.append(feeddata)
    if uid == "0":
        return [0,nouid_rls]
    else:
        all_feed_ls = [uid,usermark,feedid_dic.keys()]
        #返回:有uid的评论数据,所有的评论id
        return [1,[existuid_rls,all_feed_ls]]


def merge_item_inc_num(x,y):
    dic = {1:0,2:0}
    for each in y:
        dic[each[0]] = ech[1]
    return x+'\t'+str(dic[1])+'\t'+str(dic[2])

if __name__ == "__main__":
    if sys.argv[1] == '-h':
        comment = '-gen_his_user_feedid \t argv[2]:output dir \t 提取库里uid feedid' + \
                  '-gen_data_inc \t argv[2]:his_item_feed_file argv[3]:data path to insert argv[4-5-6]:[all_feedid new_feedid data] output dir \t 根据his_item_feed_file,每日新增评论去重，过滤库里已有\n'

        print comment

    elif sys.argv[1] == '-gen_his_user_feedid':
        sc = SparkContext(appName="gen_his_user_feedid")
        hive_dbpath = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev/ds=*/*"
        rdd_uidfeedid = sc.textFile(hive_dbpath)\
                .map(lambda x: gen_uid_feedid(x)) \
                .filter(lambda x:x!=None)\
                .groupByKey(100) \
                .mapValues(list)\
                .map(lambda (x,y):[x,[1,'\001'.join(y)]])
        rdd_uidmark = sc.textFile("/user/yarn/taobao/taobao.uidmark.24")\
                .map(lambda x:x.strip().split("\t"))\
                .filter(lambda x:len(x) == 2)\
                .map(lambda x:[x[0],[2,x[1]]])

        rdd_uidfeedid.union(rdd_uidmark)\
                .groupByKey()\
                .map(lambda (x,y):gen_uid_mark_feedid(x,y))\
                .saveAsTextFile(sys.argv[2])
        sc.stop()

    elif sys.argv[1] == '-gen_data_inc':

        his_mark_feedid = sys.argv[2]
        new_data_input_path = sys.argv[3]
        new_mark_feedid_save_path = sys.argv[4]
        inc_item_num_save_path = sys.argv[5]
        inc_data_save_path=sys.argv[6]
        user_save_path=sys.argv[7]
        nouid_feed_save_path=sys.argv[8]
        sc = SparkContext(appName="gen_cmt_inc "+new_data_input_path)
        # rdd_his_feed:return [userid,[0,[feedid1,feedid2]]]

        #历史mark uid feedid 库
        rdd_his = sc.textFile(his_mark_feedid)\
                    .map(lambda x:x.strip().split("\001"))\
                    .map(lambda x:[x[1],[0,x[0],x[2:]]])

        #新采数据
        rdd_new_data = sc.textFile(new_data_input_path)\
                    .filter(lambda x: 'SUCCESS' in x) \
                    .map(lambda x: parse_cmt_v3(x)) \
                    .filter(lambda x: x != None)
        #评论数据
        rdd_new_feed_data = rdd_new_data.map(lambda (x,y,z):x)
        #用户数据
        rdd_new_user_data = rdd_new_data.map(lambda (x,y,z):y)
        #今日新采itemid, [x,[2,1]] 2表示计算位 1表示出现标识位
        rdd_today_crawl_item = rdd_new_data.map(lambda (x,y,z):z)\
                                        .filter(lambda x:x!="")\
                                        .map(lambda x:[x,[2,1]])

        #存储新采用户数据
        '''
        rdd_new_user_data.flatMap(lambda x:x)\
                    .distinct()\
                    .saveAsTextFile(user_save_path)
        '''

        rdd_new = rdd_new_feed_data.flatMap(lambda x:x)\
                    .groupByKey()\
                    .map(lambda (x,y):[x,[1,y]])

        rdd_res = rdd_his.union(rdd_new)\
                .groupByKey()\
                .map(lambda (x,y):clean_data_by_his_mark_feedid(x,y))

        #存储新增无uid评论数据
        '''
        rdd_res_nouid = rdd_res.filter(lambda (x,y):x == 0)\
                            .map(lambda (x,y):y)\
                            .flatMap(lambda x:x)\
                            .saveAsTextFile(nouid_feed_save_path)
        '''
        #cache 有效数据
        rdd_res_valid = rdd_res.filter(lambda (x,y):x == 1).map(lambda (x,y):y)
        rdd_res_valid.cache()

        #计算新的feedid库
        rdd_all_feedid = rdd_res_valid.map(lambda (existuid_rls,all_feed_ls):all_feed_ls)\
                                    .map(lambda (x,y,z):x+'\001'+y+'\001'+"\001".join(z))\
                                    .coalesce(300)

        #计算新增评论数据
        rdd_inc_data = rdd_res_valid.map(lambda (existuid_rls,all_feed_ls):existuid_rls)\
                    .flatMap(lambda x:x)\
                    .coalesce(min(rdd_res_valid.getNumPartitions(),300))

        #计算新增商品各标志位
        rdd_inc_item_num = rdd_inc_data.map(lambda x:(x[0],1))\
                    .reduceByKey(lambda x,y:x+y)\
                    .map(lambda (x,y):[x,[1,y]])\
                    .union(rdd_today_crawl_item)\
                    .groupByKey()\
                    .map(lambda (x,y):merge_item_inc_num(x,y))\
                    .coalesce(100)

        rdd_all_feedid.saveAsTextFile(new_mark_feedid_save_path)
        rdd_inc_data.saveAsTextFile(inc_data_save_path)
        rdd_inc_item_num.saveAsTextFile(inc_item_num_save_path)



'''
    elif sys.argv[1] == '-gen_data_inc':
        sc = SparkContext(appName="gen_cmt_inc "+sys.argv[3])
        # rdd_his:return [itemid,[0,[feedid1,feedid2]]]
        rdd_his = sc.textFile(sys.argv[2])\
                    .map(lambda x:x.strip().split("\001"))\
                    .map(lambda x:[x[0],[0,x[1:]]])

        # rdd_new:return [item_id,[1,[[feedid1,feeddata1],[feedid2,feeddata2]]]]
        rdd_new = sc.textFile(sys.argv[3]) \
            .filter(lambda x: 'SUCCESS' in x) \
            .map(lambda x: parse_cmt_new(x)) \
            .filter(lambda x: x != None)\
            .flatMap(lambda x:x)\
            .groupByKey()\
            .map(lambda (x,y):[x,[1,y]])
            #.map(lambda (x,y):[x,[1,uniq_cmt(y)]])

            #groupByKey(120) before setting some oom error

        rdd_res = rdd_his.union(rdd_new)\
                .groupByKey()\
                .map(lambda (x,y):clean_data_by_hisfeedid(x,y))

        rdd_res.cache()

        rdd_all_feedid = rdd_res.map(lambda x:x[1])\
                    .map(lambda x:"\001".join(x))\
                    .coalesce(300)

        rdd_inc_feedid_num = rdd_res.map(lambda (x,y,z,flag):(y,z,flag))\
                    .map(lambda (y,z,flag):y[0]+'\t'+str(len(y)-len(z))+'\t'+str(len(z)-1)+'\t'+flag)\
                    .coalesce(100)

        rdd_data = rdd_res.map(lambda x:x[0])\
                    .flatMap(lambda x:x)\
                    .coalesce(min(rdd_res.getNumPartitions(),300))

        rdd_all_feedid.saveAsTextFile(sys.argv[4])
        rdd_inc_feedid_num.saveAsTextFile(sys.argv[5])
        rdd_data.saveAsTextFile(sys.argv[6])

        sc.stop()
'''

#pyspark --total-executor-cores  120 --executor-memory  10g --driver-memory 10g cmt_inc_baatch.py -gen_data_inc \
# /data/develop/ec/tb/cmt/feedid/all_uid_mark_feedids.20160316/part* /commit/comments/20160317/*  /data/develop/ec/tb/cmt/feedid/all_uid_mark_feedids.20160317 \
# /data/develop/ec/tb/cmt/feedid/inc_item_num.20160317 /data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.20160317 /data/develop/ec/tb/cmt/user/user.20160317 \
# /data/develop/ec/tb/cmt/tmpdata.nouid/nouid_cmt_inc_data.20160317
