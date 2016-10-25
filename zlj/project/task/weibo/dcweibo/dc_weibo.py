#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkConf
import rapidjson as json
conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
conf.set("spark.kryoserializer.buffer.mb", "1024")
conf.set("spark.akka.frameSize", "100")
conf.set("spark.network.timeout", "1000s")
conf.set("spark.driver.maxResultSize", "8g")

sc = SparkContext(appName="user_cattags", conf=conf)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
        return res
    else: return res
# coding=utf8
import sys
from email  import utils
import re

from datetime import datetime

def fun1(line):
    if "\001" not in line :return None
    # ls= valid_jsontxt(line).split("\001")
    # if len(ls)!=2:return None
    mid, s = line.split("\001")
    j = json.loads(valid_jsontxt(s))
    if type(j)!=type({}):return None
    res=()
    for wb in j['reposts']:
        if 'retweeted_status' not in wb:
            continue
        rs = wb['retweeted_status']
        res = (rs['idstr'] ,'\001'.join([ str(i) for i in [
            rs['idstr'],
            str(rs['user']['id']),
            rs['created_at'],
            valid_jsontxt(rs['text'])
        ] ])
            )
        break
    return res


def fun(line):
    try:
        ob= json.loads(line)
        return [str(ob['uid']),','.join([str(i) for i  in ob['ids']])]
    except:return None
sc.textFile('/data/develop/sinawb/rel_fri.json.20160401').map(lambda x:fun(x)).filter(lambda x:x!=None)\
    .map(lambda x:'\001'.join(x)).saveAsTextFile('/user/zlj/tmp/rel_fri.json')


def fun(line):
    if "\001" not in line :return None
    # ls= valid_jsontxt(line).split("\001")
    # if len(ls)!=2:return None
    mid, s = line.split("\001")
    j = json.loads(valid_jsontxt(s))
    if type(j)!=type({}):return None
    rs=[]
    for wb in j['reposts']:
        if 'retweeted_status' not in wb:
            continue
        date_tz = utils.parsedate_tz(wb['created_at'])
        end_date = datetime(*date_tz[:6])

        date_tz = utils.parsedate_tz(
            wb['retweeted_status']['created_at'])
        start_date = datetime(*date_tz[:6])
        # time=[start_date.hour start_date.minute]
        if start_date.year<2015:continue

        t = int((end_date - start_date).total_seconds())

        #wstr1 = '\001'.join([mid, wb['retweeted_status']['user']['idstr'], wb['user']['idstr'], str(t)])

        name = wb['retweeted_status']['user']['screen_name']
        # re_s = re.findall(u'//@(.*?)[:：]{1}', wb['text'], re.UNICODE)
        # if len(re_s) != 0:
        #     name = re_s[0]
        # wstr1 = '\001'.join(
        #     [mid, name.encode('utf8'), wb['user']['screen_name'].encode('utf8'), str(t)])
        # wf_1.write(wstr1 + '\n')

        txt = valid_jsontxt(wb['text']).replace('转发微博', '').replace('\n', '') ##.split('//@')[0]
        if '//@' in txt:
            next_user_name=valid_jsontxt(txt).split('//@')[1].split(':')[0]
        else: next_user_name=""
        wstr2 = '\001'.join( [valid_jsontxt(i) for i in [mid,
                                                         name,wb['retweeted_status']['user']['idstr'],
                                                         wb['user']['screen_name'],wb['user']['idstr'],
                                                         next_user_name ,
                                                         str(t), txt]])
        rs.append(wstr2)

    return  rs

def try_fun(line):
    try:
        return fun(line)
    except: return None

def try_fun1(line):
    try:
        return fun1(line)
    except: return None
sc.textFile('/commit/weibo_dc').map(lambda x:try_fun(x)).filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/commit/weibo_dc_parse2015')


sc.textFile('/commit/weibo_dc/')\
    .map(lambda x:try_fun1(x)).filter(lambda x:x!=None).filter(lambda x: len(x)==2).groupByKey()\
    .filter(lambda (x,y):len(list(y))>0).map(lambda (x,y): list(y)[0]).saveAsTextFile('/commit/weibo_dc_parse2015_8w_fix')

sc.textFile('/commit/weibo_dc/')\
    .map(lambda x:try_fun1(x)).filter(lambda x:x!=None ).filter(lambda x: len(x)==2).map(lambda x:x[0]+'\002'+x[1]).saveAsTextFile('/commit/weibo_dc_parse2015_8w')
# sc.textFile('/user/zlj/tmp/1').map(lambda x:fun(x)).filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/user/zlj/tmp/1_parse')



# spark-submit  --total-executor-cores  50   --executor-memory  14g  --driver-memory 14g  extract_wb_spark.py  -filter_fri_by_big_uid  /hive/warehouse/wlbase_dev.db/t_zlj_dc_weibodata_3w_username_id  /commit/weibo_dc_parse2015_link

# 关系数据
def fun1(line,tel_map):
    ob=json.loads(line)
    if type(ob)!=type({}):return None
    uid=str(ob.get('uid'))
    if tel_map.has_key(uid):
        uid=tel_map.get(uid)
    ids=  ob.get('ids')
    ls=[]
    if ids ==None:return None
    ids=[str(i) for  i in ob.get('ids')]
    for id in ids:
        if  tel_map.has_key(id):
            ls.append(tel_map.get(id))
    return str(uid)+'\t'+'\001'.join(ls)


tel_map  = sc.broadcast(sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_dc_weibodata_3w_username_id').map(lambda x:(x.split('\001')[0],x.split('\001')[1])).collectAsMap())

sc.textFile('/commit/weibo_dc_parse2015_link').map(lambda x:fun1(x,tel_map.value)).filter(lambda x:x is not None)\
    .repartition(100).saveAsTextFile('/commit/weibo_dc_parse2015_link_filter')

# wf_2.close()
# wf_1.close()


def fun(line):
   ob= json.loads(line)
   uid =ob['uid']
   ls=[]
   for i in ob.keys():
       if 'uid' not in i:
           ls.append((i,uid))
   return ls

rdd=sc.textFile('/data/develop/ec/tb/cmt/uid_mark/uid_mark_freq.json.20160726/').map(lambda x:fun(x))\
    .flatMap(lambda x:x).map(lambda x:'\001'.join(x)).saveAsTextFile('/user/zlj/tmp/uid_mark_freq')

rdd1=sc.broadcast(sc.textFile('/commit/tb_comment_tz_tmp/tz.cmt').map(lambda x:(x.split('\001')[-1],x)).collectAsMap())
tel_map  = sc.broadcast(sc.textFile(sys.argv[2]).map(lambda x:(int(x.strip()),None)).collectAsMap())

def  fun1(x,map):
    k,v =x
    if map.has_key(k):
        return (v,map[k])
rs=rdd.map(lambda x:fun1(x,rdd1.value)).filter(lambda x:x !=None)
rsdd=rdd.join(rdd1).map(lambda (x,y):(y[0],y[1]))

rsdd




def fun(line):
    ob = json.loads(valid_jsontxt(line))
    if type(ob)!=type({}):return None
    if ob['count']<1:return None
    ls_cards=ob['cards']
    ls=[]
    for cards in ls_cards:
        card_group_ls=cards['card_group']
        for card_group in card_group_ls:
            mblog=card_group['mblog']
            weibo_idstr=mblog['idstr']
            text=mblog['text']
            created_timestamp=mblog['created_timestamp']
            user=mblog['user']
            user_id=user['id']
            screen_name=user['screen_name']
            ls.append('\001'.join([ str(i) for i in [user_id,screen_name,weibo_idstr,text,created_timestamp]]))
    return ls

def try_fun(line):
    try: return fun(line)
    except:return None
sc.textFile('/commit/weibo.json_1').map(lambda x:try_fun(x)).filter(lambda x:x!=None).flatMap(lambda x:x).saveAsTextFile('/commit/weibo.json_1_parse_v1')

# 转发微博

def fun(x):
    id,ids=x.split('\001')
    ls=[]
    for i in ids.split(','):
        ls.append(id+' '+i)
    return ls
sc.textFile('/user/zlj/algo/part-000*').map(lambda x:fun(x)).flatMap(lambda x:x).saveAsTextFile('/user/zlj/algo/part_flatmap')

sc.textFile('/user/zlj/tmp/t_base_weibo_user_fri_tel/part-000**').map(lambda x:fun(x)).flatMap(lambda x:x).saveAsTextFile('/user/zlj/algo/part_flatmap100')