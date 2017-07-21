
def splitAndFilter(fields):
    data = fields.split("\001")
    id = data[0]
    ids = data[1]
    return (id, ids)

def exchangAndFilter(a,b):
    if v.has_key(b):
        return (b,a)
    else:
        return None

fri = sc.textFile("/hive/warehouse/wl_base.db/t_base_weibo_user_fri/ds=20161106") \
    .map(lambda a: splitAndFilter(a)).filter(lambda a: a is not None).flatMapValues(lambda a: a.split(",")).map(lambda (a,b):exchangAndFilter(a,b)).filter(lambda a: a is not None)
import re


def split(str):
    data = str.split("\001")
    if len(data) <= 22: return None
    id = data[0]
    name = data[3]
    verified = data[22]
    if "True" in verified and len(re.findall("(程序)(猿|员)|^IT[^0-9A-Za-z]",name)) !=0:
        return (id, None)
    else:
        return None


new = sc.textFile("/hive/warehouse/wl_base.db/t_base_weibo_user_new/ds=20161123").map(lambda a: split(a)).filter(lambda a:a is not None)

v = sc.broadcast(new.collectAsMap()).value
#
# fri.join(new).map(lambda (a,(b,c)): b).distinct().saveAsTextFile("/user/lel/results/weibo_find_it1")
# create table wl_service.t_lel_find_IT
# as
# select a.id from
# (select id,vid from  wl_base.t_base_weibo_user_fri  LATERAL VIEW explode(split(ids,",")) tmptable  as vid ) a
# left semi join
# (select id from wl_base.t_base_weibo_user_new where (name regexp '(程序)(猿|员)' or name regexp '^IT[^0-9A-Za-z]') and verified = 'True') b
# on a.vid = b.id
