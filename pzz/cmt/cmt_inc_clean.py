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
    return [ls[0], ls[2]]


def parse_cmt_new(line_s):
    line = valid_jsontxt(line_s)
    ts = line[:line.find('\t')]
    # ts ='1445270400'
    # ts=str(time.mktime(datetime.datetime.now().timetuple()))
    json_txt = line.strip()[line.find('2(') + 2:-1]
    ob = json.loads(json_txt)
    if type(ob) == type({}) and ob["data"].has_key("rateList"):
        data = ob['data']
        list = []
        for value in data['rateList']:
            l = []
            itemid = value.get('auctionNumId', '-')
            l.append(itemid)
            l.append(value.get('auctionTitle', '-'))
            l.append(value.get('id', '-'))
            l.append(value.get('userId', '-'))
            # l.append(data.get('userStar'))
            feedback = value.get('feedback', '-').replace('\001', '').replace('\n', '')
            l.append(valid_jsontxt(feedback))
            date = value.get('feedbackDate', '-')
            l.append(date)
            l.append(value.get('annoy', '-'))
            l.append(ts)
            l.append(date.replace('-', ''))
            # l.append(str(time.mktime(datetime.datetime.now().timetuple())))
            list.append([itemid,"\001".join(l)])
        return list
    return None

def uniq_cmt(ls):
    rls = []
    feedid_dic = {}
    for one in ls:
        feedid = one.split("\001")[2]
        if feedid_dic.has_key(feedid) == False:
            rls.append(one)
            feedid_dic[feedid] = None
    return rls


if __name__ == "__main__":
    if sys.argv[1] == '-h':
        comment = '-gen_data_inc 每日新增评论数据去重，库里已有feedid增量过滤'
        print comment
        print 'argvs:\n argv[2]: hive partition path\n argv[3]: today feed inc file'

    elif sys.argv[1] == '-gen_his_item_feed_file':
        sc = SparkContext(appName="gen_his_item_feed_file")
        sc.textFile(sys.argv[2]) \
            .map(lambda x: gen_item_feedid(x)) \
            .groupByKey(50)\
            .map(lambda (x,y):x+"\001"+"\001".join(y))\
            .saveAsTextFile(sys.argv[3])
        sc.stop()

    elif sys.argv[1] == '-gen_data_inc':
        sc = SparkContext(appName="gen_data_inc")
        # rdd_his:return [itemid,[0,[feedid1,feedid2]]]

'''
        # rdd_new:return [item_id,[1,[feeddata1,feeddata2]]]
        rdd_new = sc.textFile(sys.argv[3]) \
            .filter(lambda x: 'SUCCESS' in x) \
            .map(lambda x: parse_cmt_new(x)) \
            .filter(lambda x: x != None)\
            .flatMap(lambda x:x)\
            .groupByKey(120)\
            .map(lambda (x,y):[x,[1,uniq_cmt(y)]])
'''
        sc.stop()