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


def f(line):
    ss = line.strip().split("\t",3)
    if len(ss) != 4: return [None]
    # user_id = ss[1]
    ts = ss[0]
    txt = valid_jsontxt(ss[3])
    if txt == "": return [None]
    ob = json.loads(txt)
    if type(ob) != type({}): return [None]
    statuses = ob.get("statuses",[])
    result = []
    for statuse in statuses:
        lv = []
        user_id = statuse.get('id','-')
        mid = statuse.get('mid',"-")
        created_at = statuse.get('created_at','-')
        text = statuse.get('text',"-")
        source = statuse.get('source',"-")
        if source != "-":
            source = pq(source).text()
        favorited = statuse.get('favorited',False)
        truncated = statuse.get('truncated',False)
        thumbnail_pic = statuse.get('bmiddle_pic','-')
        geo = statuse.get('geo','-')
        if geo == None: geo = "-"
        reposts_count = statuse.get('reposts_count','-')
        comments_count = status.get('comments_count','-')
        attitudes_count = status.get('attitudes_count','-')
        weibo_type = statuse.get('visible',{}).get('type','-')#type取值，0：普通微博，1：私密微博，3：指定分组微博，4：密友微博；
        isLongText = statuse.get('isLongText',False)
        retweeted_status = status.get('retweeted_status',"-")
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
        result.append((mid,lv))
        # result.append(lv)
    return result


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


rdd_c = sc.textFile("/commit/weibo/tmp").flatMap(lambda x:f(x)).filter(lambda x:x != None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
rdd.saveAsTextFile('/user/wrt/temp/weibo_text')

#hfs -rmr /user/wrt/temp/weibo_text
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 t_base_weibo_text.py
