# coding:utf-8
import sys, rapidjson, time
import rapidjson as json
from pyspark import SparkContext


def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\001", "").replace("\n", " ")


def gen_item_feedid(line):
    ls = line.strip().split("\001")
    mission_feed_ls = map(lambda x:str(x),sorted(map(lambda x: int(x), feedid_dic.keys()))[-20:])
    if len(mission_feed_ls) < 20:
        tmpls = ['0']
        tmpls += mission_feed_ls
        mission_feed_ls = tmpls
    return ls[0]+'\t'+str(len(ls)-1)+'\t'+'\001'.join(mission_feed_ls)


def parse_cmt_new(line_s):
    line = valid_jsontxt(line_s)
    ts = line[:line.find('\t')]
    # ts ='1445270400'
    # ts=str(time.mktime(datetime.datetime.now().timetuple()))
    json_txt = line.strip()[line.find('2(') + 2:-1]
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
                l.append(value.get('auctionTitle', '-'))
                feedid = value.get('id', '-')
                int(feedid)
                l.append(feedid)
                l.append(value.get('userId', '-'))
                # l.append(data.get('userStar'))
                feedback = value.get('feedback', '-').replace('\001', '').replace('\n', '')
                l.append(valid_jsontxt(feedback))
                date = value.get('feedbackDate', '-')
                l.append(date)
                l.append(value.get('annoy', '-'))
                l.append(ts)
                date = date.replace('-', '')
                int(date)
                if len(date) != 8:
                    print "date is wrong,now is " + date
                    continue
                # l.append(str(time.mktime(datetime.datetime.now().timetuple())))
                list.append([itemid, [feedid, "\001".join(l)]])
            except Exception, e:
                print e, line
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


def clean_data_by_hisfeedid_sorted(y):
    feedid_dic = {}
    min_feedid = 0
    # 返回数据ls
    rls = []
    #任务评论id ls
    mission_feed_ls = []
    all_feed_num = 0
    #新增评论id ls
    new_item_feedid_ls = []


    #初始化feed_dic与min_feedid
    #flag:0 原feedid; 1 新增评论数据

    for flag, num, last_mession_feeds in y:
        if flag == 0:
            for feedid in last_mession_feeds:
                feedid_dic[feedid] = None
            min_feedid = min(last_mession_feeds)
            all_feed_num = num

    #数据过滤 by mission feed id
    for flag, feeds in y:
        if flag == 1:
            for feedid, feeddata in feeds:
                if feedid_dic.has_key(feedid) == False and feedid > min_feedid:
                    rls.append(feeddata)
                    new_item_feedid_ls.append(feedid)
                    feedid_dic[feedid] = None
                    all_feed_num += 1

    mission_feed_ls = map(lambda x:str(x),sorted(map(lambda x: int(x), feedid_dic.keys()))[-20:])
    if len(mission_feed_ls) < 20:
        tmpls = ['0']
        tmpls += mission_feed_ls
        mission_feed_ls = tmpls
    #return [itemid,新增数据ls，任务产出feedid ls，所有feed数量，新增feed数量]
    return [rls, mission_feed_ls, all_feed_num, len(new_item_feedid_ls)]

    #return [rls,all_feed_ls,new_item_feedid_ls]


if __name__ == "__main__":
    if sys.argv[1] == '-h':
        comment = '-gen_mission_feedid_file \t argv[2]:hive db file,argv[3]:output dir \t 生成任务feedid' + \
                  '-gen_data_inc \t argv[2]:his_item_feed_file argv[3]:data path to insert argv[4-5-6]:[all_feedid new_feedid data] output dir \t 根据his_item_feed_file,每日新增评论去重，过滤库里已有\n'

        print comment

    elif sys.argv[1] == '-gen_mission_feedid_file':
        sc = SparkContext(appName="gen_mission_feedid_file")
        hive_dbpath = sys.argv[2]
        sc.textFile(hive_dbpath) \
            .map(lambda x: gen_item_feedid(x)) \
            .saveAsTextFile(sys.argv[3])
        sc.stop()

    elif sys.argv[1] == '-gen_data_inc':
        sc = SparkContext(appName="gen_cmt_inc " + sys.argv[3])

        # rdd_his:imput itemid + '\t' + num + '\t' + '\001'.join(feedid)
        # rdd_his:return [itemid,[0,num,[feedid1,feedid2]]]
        rdd_his = sc.textFile(sys.argv[2]) \
            .map(lambda x: x.strip().split("\t")) \
            .map(lambda (itemid, allnum, fidls): [itemid, [0, allnum, fidls.split("\001")]])

        # rdd_new:return [item_id,[1,[[feedid1,feeddata1],[feedid2,feeddata2]]]]
        rdd_new = sc.textFile(sys.argv[3]) \
            .filter(lambda x: 'SUCCESS' in x) \
            .map(lambda x: parse_cmt_new(x)) \
            .filter(lambda x: x != None) \
            .flatMap(lambda x: x) \
            .groupByKey() \
            .map(lambda (x, y): [x, [1, y]])
        # .map(lambda (x,y):[x,[1,uniq_cmt(y)]])

        #groupByKey(120) before setting some oom error

        rdd_res = rdd_his.union(rdd_new) \
            .groupByKey() \
            .map(lambda (x, y): (x, clean_data_by_hisfeedid_sorted(y)))
        #rdd_res: return (x,[rls,mission_feed_ls,all_feed_num,len(new_item_feedid_ls)])

        #new_mission_feedid return: itemid + '\t' + allnum + '\t' + '\001'.join(fidls)
        rdd_new_mission_feedid = rdd_res.map(lambda (itemid, res): (itemid, res[2], res[1])) \
            .map(lambda (x, y, z): x + '\t' + str(y) + '\t' + '\001'.join(z)) \
            .coalesce(200)

        #inc_feed_num return: itemid +'\t'+allnum + '\t' + incnum
        rdd_inc_feed_num = rdd_res.map(lambda (itemid, res): (itemid, res[2] - res[3]), res[3]) \
            .map(lambda (x, y, z): x + '\t' + str(y) + '\t' + str(z)) \
            .coalesce(100)

        #rdd_data reutrn:data save
        rdd_data = rdd_res.map(lambda (itemid, res): res[0]) \
            .flatMap(lambda x: x) \
            .coalesce(min(rdd_res.getNumPartitions(), 300))

        rdd_all_feedid.saveAsTextFile(sys.argv[4])
        rdd_inc_feedid_num.saveAsTextFile(sys.argv[5])
        rdd_data.saveAsTextFile(sys.argv[6])

        sc.stop()