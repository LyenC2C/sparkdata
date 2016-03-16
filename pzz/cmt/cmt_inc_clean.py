# coding:utf-8
import sys, rapidjson, time
import rapidjson as json
from pyspark import SparkContext


def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")


def gen_item_feedid(line):
    ls = line.strip().split("\001")
    return [ls[0], ls[2]]


def parse_cmt_new(line_s):
    line = valid_jsontxt(line_s)
    ts = line[:line.find('\t')]
    # ts ='1445270400'
    # ts=str(time.mktime(datetime.datetime.now().timetuple()))
    #json_txt = line.strip()[line.find('2(') + 2:-1]
    #0128 changed
    #json_txt = line.strip()[line.find('3(') + 2:-1]
    json_txt = line.strip()[line.find('({"') + 1:-1]
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
        comment = '-gen_his_item_feed_file \t argv[2]:hive db file,argv[3]:output dir \t 提取库里商品feedid' + \
                  '-gen_data_inc \t argv[2]:his_item_feed_file argv[3]:data path to insert argv[4-5-6]:[all_feedid new_feedid data] output dir \t 根据his_item_feed_file,每日新增评论去重，过滤库里已有\n'

        print comment

    elif sys.argv[1] == '-gen_his_item_feed_file':
        sc = SparkContext(appName="gen_his_item_feed_file")
        hive_dbpath = sys.argv[2]
        sc.textFile(hive_dbpath) \
            .map(lambda x: gen_item_feedid(x)) \
            .groupByKey(100) \
            .map(lambda (x, y): x + "\001" + "\001".join(y)) \
            .saveAsTextFile(sys.argv[3])
        sc.stop()

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