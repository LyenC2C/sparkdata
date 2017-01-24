#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
from pyquery import PyQuery as pq

sc = SparkContext(appName="t_base_weibo_text")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")


def error_out(line):
    ss = line.strip().split("\t",3)
    if len(ss) != 4: return None
    user_id = ss[1]
    ts = ss[0]
    page = ss[2]
    txt = valid_jsontxt(ss[3])
    if txt == "": return None
    ob = json.loads(txt)
    if type(ob) != type({}): return None
    if ob.has_key("error") and page == '1' : return user_id
    total_number = str(ob.get("total_number",0))
    if total_number == '0':
        return None
    statuses = ob.get("statuses",[])
    if statuses == [] and total_number <> '0': return user_id
    return None


def f(line):
    ss = line.strip().split("\t",3)
    if len(ss) != 4: return [None]
    user_id = ss[1]
    ts = ss[0]
    txt = valid_jsontxt(ss[3])
    if txt == "": return [None]
    ob = json.loads(txt)
    if type(ob) != type({}): return [None]
    if ob.has_key("error"): return [None]
    if str(ob.get("total_number",0)) == '0':
        return [None]
    statuses = ob.get("statuses",[])
    result = []
    for statuse in statuses:
        lv = []
        # user_id = statuse.get('uid','-')
        mid = statuse.get('mid',"-")
        created_at = statuse.get('created_at','-')
        text = statuse.get('text',"-")
        source = valid_jsontxt(statuse.get('source',"-"))
        if source == "": source = "-"
        if source != "-":
            source = pq(source).text()
        favorited = statuse.get('favorited',False)
        if favorited == True: favorited = 1
        else: favorited = 0
        truncated = statuse.get('truncated',False)
        if truncated == True: truncated = 1
        else: truncated = 0
        isLongText = statuse.get('isLongText',False)
        if isLongText == True: isLongText = 1
        else: isLongText = 0
        thumbnail_pic = statuse.get('bmiddle_pic','-')
        geo = statuse.get('geo','-')
        if geo == None: geo = "-"
        reposts_count = statuse.get('reposts_count','-')
        comments_count = statuse.get('comments_count','-')
        attitudes_count = statuse.get('attitudes_count','-')
        weibo_type = statuse.get('visible',{}).get('type','-')#type取值，0：普通微博，1：私密微博，3：指定分组微博，4：密友微博；
        retweeted_status = statuse.get('retweeted_status',"-")
        if retweeted_status != "-":
            ori_uid = retweeted_status.get("uid","-")
            ori_mid = retweeted_status.get("mid",'-')
            ori_text = retweeted_status.get("text","-")
            retweeted_status = valid_jsontxt(ori_uid) + "\002" + valid_jsontxt(ori_mid) + "\002" + valid_jsontxt(ori_text)
        lv.append(mid)
        lv.append(user_id)
        lv.append(created_at)
        lv.append(text)
        lv.append(source)
        lv.append(favorited)
        lv.append(truncated)
        lv.append(thumbnail_pic)
        lv.append(geo)
        lv.append(reposts_count)
        lv.append(comments_count)
        lv.append(attitudes_count)
        lv.append(weibo_type)
        lv.append(isLongText)
        lv.append(retweeted_status)
        lv.append(ts)
        # result.append((mid,lv))
        result.append("\001".join([valid_jsontxt(ln) for ln in lv]))
        # result.append(lv)
    return result
    # return result


def quchong(x, y):
    max = 0
    item_list = y
    for ln in item_list:
        if int(ln[-1]) > max:
                max = int(ln[-1])
                y = ln
    result = y
    lv = []
    for ln in result:
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)

rdd1 = sc.textFile("/commit/weibo/user_weibo/wrt_tmp2/*/*")
rdd_c = rdd1.flatMap(lambda x:f(x)).filter(lambda x:x != None)
# rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
# rdd_error = rdd1.map(lambda x:error_out(x)).filter(lambda x:x != None)
# rdd_error.saveAsTextFile('/user/wrt/temp/weibo_text_error')
rdd_c.saveAsTextFile('/user/wrt/temp/weibo_text')

#hfs -rmr /user/wrt/temp/weibo_text
# spark-submit  --executor-memory 20G  --driver-memory 20G  --total-executor-cores 300 t_base_weibo_text.py
#LOAD DATA  INPATH '/user/wrt/temp/weibo_text' INTO TABLE t_base_weibo_text PARTITION (ds='20161103');
#commit下的20161026的数据是新的1.8亿的微博用户数据
#ds='20161103'是将10.26到11.01的1.8亿打通电话的微博用户的微博内容存储分区，这几天有8千万左右数据量，后面全部采集完毕依然入这个分区
#20161126是所有1.8亿打通电话微博数据，然而只有9千万数据打通。

