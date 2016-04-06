# coding:utf-8
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
def parse_cmt_v3(line_s):
    line = valid_jsontxt(line_s)
    ls = line.strip().split("\t")
    if len(ls) != 6:
        return None
    ts = ls[0]
    json_txt = ls[5][11:-1]
    ob = json.loads(json_txt)
    if type(ob) == type({}) and ob.has_key("data") and ob["data"].has_key("rateList"):
        data = ob['data']
        list = []
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
                int(userid)
                l.append(userid)

                # l.append(data.get('userStar'))
                feedback = value.get('feedback', '-').replace('\t', '')
                l.append(valid_jsontxt(feedback))
                date = value.get('feedbackDate', '-').replace(".","-")
                l.append(date)
                annoy = value.get('annoy', '-')
                l.append(annoy)
                l.append(ts)
                date = date[:10].replace('-', '')
                int(date)
                if len(date) != 8:
                    print "date is wrong,now is "+date
                    continue
                # l.append(str(time.mktime(datetime.datetime.now().timetuple())))
                list.append([itemid, [feedid,"\001".join(l)]])
            except Exception,e:
                print e,line
        return list
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

def clean_data_by_hisfeedid(itemid,y):
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
        sc = SparkContext(appName="gen_cmt_inc "+sys.argv[3])

        # rdd_his_feed:return [userid,[0,[feedid1,feedid2]]]
        rdd_his_feed = sc.textFile(sys.argv[2])\
                        .map(lambda x:x.strip().split("\001"))\
                        .map(lambda x:[x[0],x[0,x[1:]]])

        rdd_new_data = sc.textFile(sys.argv[3])\
                    .filter(lambda x: 'SUCCESS' in x) \
                    .map(lambda x: parse_cmt_v3(x)) \
                    .filter(lambda x: x != None)\
                    .flatMap(lambda x:x)\
                    .groupByKey()\
                    .map(lambda (x,y):[x,[1,y]])
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