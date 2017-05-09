import rapidjson as json
import datetime,time
from pyspark import SparkContext

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

def date_to_ts(date_str):
    if date_str in "\\N": return "\\N"
    try:
        d = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
        t = d.timetuple()
        ts = str(int(time.mktime(t)))
        return ts
    except ValueError as e:
        d = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        t = d.timetuple()
        ts = str(int(time.mktime(t)))
        return ts

def take_out_dot(s):
    return s[:s.index(".")] if '.' in s else s


def process(line):
    jsonStr = valid_jsontxt(line.strip())
    ob = json.loads(jsonStr)
    repeat = ob.get("repeat","\\N")
    success = ob.get("success","\\N")
    app_key = ob.get("app_key","\\N")
    create_time = ob.get("create_time","\\N")
    if '.' in create_time:
        create_time = create_time[:create_time.index('.')]
    ts_create = date_to_ts(create_time)
    params = ob.get("params",{})
    if params:
        sign = params.get("sign","\\N")
        ts_request= params.get("t","\\N")
        if '.' in ts_request:
            ts_request = ts_request[:ts_request.index('.')]
        app_key_param = params.get("app_key","\\N")
        params_common_key = ["t","sign","app_key"]
        params_less = ','.join([key+":" +take_out_dot(valid_jsontxt(params[key])) for key in params if key not in params_common_key])
        params_all = ','.join([key+":" +take_out_dot(valid_jsontxt(params[key]))for key in params] )
    else:
        return None
    interface = ob.get("interface","\\N")
    match = ob.get("match","\\N")
    api_type = ob.get("api_type","\\N")
    return "\001".join([valid_jsontxt(i)for i in [repeat,success,app_key,app_key_param,ts_create,ts_request,params_less,params_all,sign,interface,match,api_type]])

sc = SparkContext(appName="data_backflow")

data = sc.textFile("/user/lel/data/datamart_backflow/api_record.json") \
    .map(lambda a: process(a)) \
    .distinct() \
    .filter(lambda a: a is not None) \
    .saveAsTextFile("/user/lel/temp/data_backflow")




