# coding:utf-8
import sys, rapidjson, time
import rapidjson as json
from pyspark import SparkContext


def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\001", "").replace("\n", " ")


def feediddate(line):
    line = valid_jsontxt(line)
    ts = line[:line.find('\t')]
    json_txt = line.strip()[line.find('{'):-1]
    try:
        rls = []
        j = rapidjson.loads(json_txt)
        if j != 0.0 and j["data"].has_key("rateList"):
            ils = j["data"]["rateList"]
            for i in ils:
                rls.append(i["id"] + '\t' + i["feedbackDate"].replace("-", ""))
        return rls
    except:
        return None


def uidmark(line):
    line = valid_jsontxt(line)
    ts = line[:line.find('\t')]
    json_txt = line.strip()[line.find('{'):-1]
    try:
        rls = []
        j = rapidjson.loads(json_txt)
        if j != 0.0 and j["data"].has_key("rateList"):
            ils = j["data"]["rateList"]
            for i in ils:
                rls.append(i["userId"] + '\t' + i["userMark"])
        return rls
    except Exception,e:
        print line.strip(),e
        return None


def parse_cmt(line_s):
    line = valid_jsontxt(line_s)
    # ts =line[:line.find('\t')]
    ts = '1445270400'
    # ts=str(time.mktime(datetime.datetime.now().timetuple()))
    json_txt = line.strip()[line.find('2(') + 2:-1]
    ob = json.loads(json_txt)
    if type(ob) == type({}) and ob["data"].has_key("rateList"):
        data = ob['data']
        list = []
        for value in data['rateList']:
            l = []
            l.append(value.get('auctionNumId', '-'))
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
            list.append("\001".join(l))
        return list


def parse_cmt_new(line_s):
    line = valid_jsontxt(line_s)
    ts = line[:line.find('\t')]
    # ts ='1445270400'
    #ts=str(time.mktime(datetime.datetime.now().timetuple()))
    json_txt = line.strip()[line.find('2(') + 2:-1]
    ob = json.loads(json_txt)
    if type(ob) == type({}) and ob["data"].has_key("rateList"):
        data = ob['data']
        list = []
        for value in data['rateList']:
            l = []
            l.append(value.get('auctionNumId', '-'))
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
            list.append("\001".join(l))
        return list


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
        comment = '-feediddate 抽取ｆｅｅｄｉｄ和ｄａｔｅ \n\
				   -uidmark 抽取uid 和加密id\n\
				   -hiveformat hiveforamt'
        print comment
        print 'argvs:\n argv[1]:file or dir input\n argv[2]:dir output'

    elif sys.argv[1] == '-feediddate':
        sc = SparkContext(appName="feedid_date")
        rdd = sc.textFile(sys.argv[2])
        rdd.map(lambda x: feediddate(x)) \
            .filter(lambda x: x != None) \
            .flatMap(lambda x: x) \
            .distinct() \
            .saveAsTextFile(sys.argv[3])
        sc.stop()

    elif sys.argv[1] == '-uidmark':
        sc = SparkContext(appName="uidmark")
        rdd = sc.textFile(sys.argv[2])
        rdd.map(lambda x: uidmark(x)) \
            .filter(lambda x: x is not None) \
            .flatMap(lambda x: x) \
            .distinct() \
            .saveAsTextFile(sys.argv[3])
        sc.stop()

    elif sys.argv[1] == '-hiveformatold':
        sc = SparkContext(appName="hiveformatold")  # /data/develop/ec/tb/cmt/*
        rdd1 = sc.textFile("/data/develop/ec/tb/cmt/*") \
            .filter(lambda x: 'SUCCESS' in x) \
            .map(lambda x: parse_cmt(x)) \
            .filter(lambda x: x != None)
        # /data/develop/ec/tb/cmt_new/*
        rdd2 = sc.textFile("/data/develop/ec/tb/cmt_new/*") \
            .filter(lambda x: 'SUCCESS' in x) \
            .map(lambda x: parse_cmt_new(x)) \
            .filter(lambda x: x != None)
        rdd3 = rdd1.union(rdd2).flatMap(lambda x: x) \
            .map(lambda x: (x.split("\001")[0], x)) \
            .groupByKey() \
            .map(lambda (x, y): uniq_cmt(y)) \
            .flatMap(lambda x: x)

        rdd2013 = rdd3.filter(lambda x: x.split("\001")[5][:4] == "2013") \
            .saveAsTextFile("/data/develop/ec/tb/cmt_res_tmp/res2013")
        rdd2014 = rdd3.filter(lambda x: x.split("\001")[5][:4] == "2014") \
            .saveAsTextFile("/data/develop/ec/tb/cmt_res_tmp/res2014")
        rdd2015 = rdd3.filter(lambda x: x.split("\001")[5][:4] == "2015") \
            .saveAsTextFile("/data/develop/ec/tb/cmt_res_tmp/res2015")

        sc.stop()

    elif sys.argv[1] == '-hiveformatnew':
        sc = SparkContext(appName="hiveformatnew")  # /data/develop/ec/tb/cmt/*
        in_f = sys.argv[2]
        out_dir = sys.argv[3]
        rdd1 = sc.textFile(in_f) \
            .filter(lambda x: 'SUCCESS' in x) \
            .map(lambda x: parse_cmt_new(x)) \
            .filter(lambda x: x != None)\
            .flatMap(lambda x:x)\
            .map(lambda x: (x.split("\001")[0], x)) \
            .groupByKey() \
            .map(lambda (x, y): uniq_cmt(y)) \
            .flatMap(lambda x: x)
        rdd1.saveAsTextFile(out_dir)

        sc.stop()