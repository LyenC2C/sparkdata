import rapidjson as json
import datetime, time
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
    repeat = ob.get("repeat", "\\N")
    success = ob.get("success", "\\N")
    app_key = ob.get("app_key", "\\N")
    create_time = ob.get("create_time", "\\N")
    if '.' in create_time:
        create_time = create_time[:create_time.index('.')]
    ts_create = date_to_ts(create_time)
    params = ob.get("params", {})
    result = ob.get("result",{})
    if not isinstance(params,dict) or not isinstance(result,dict):return None
    params_str = ','.join([key + ":" + take_out_dot(valid_jsontxt(params[key])) for key in params])
    result_str = ','.join([key + ":" + take_out_dot(valid_jsontxt(result[key])) for key in result])
    interface = ob.get("interface", "\\N")
    match = ob.get("match", "\\N")
    api_type = ob.get("api_type", "\\N")
    return "\001".join([valid_jsontxt(i) for i in
                        [repeat, success, app_key, ts_create, params_str, result_str,str(params),str(result),
                         interface, match, api_type]])


sc = SparkContext(appName="data_backflow")

data = sc.textFile("/commit/data_backflow/jinrongyonghuhuaxiang.json") \
    .map(lambda a: process(a)) \
    .distinct() \
    .filter(lambda a: a is not None) \
    .saveAsTextFile("/user/lel/temp/backflow_portrait")
