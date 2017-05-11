import rapidjson as json
import datetime, time
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

def transform_df_fileds(content):
    if isinstance(content, unicode):
        res = content.encode("utf-8").decode("latin-1").encode("iso-8859-1").decode("utf-8")
    elif isinstance(content, str):
        res = content.decode("latin-1").encode("iso-8859-1").decode("utf-8")
    else:
        res = str(content)
    return res

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
    repeat = ob.get("repeat", None)
    success = ob.get("success", None)
    app_key = ob.get("app_key", None)
    create_time = ob.get("create_time", None)
    if '.' in create_time:
        create_time = create_time[:create_time.index('.')]
    ts_create = date_to_ts(create_time)
    params = ob.get("params", {})
    if params:
        sign = params.get("sign", None)
        ts_request = params.get("t", None)
        if '.' in ts_request:
            ts_request = ts_request[:ts_request.index('.')]
        app_key_param = params.get("app_key", None)
    else:
        return None
    for k,v in params.iteritems():
        params[k] = transform_df_fileds(v)
    result = json.dumps(ob.get("result", {}))
    interface = ob.get("interface", None)
    match = ob.get("match", None)
    api_type = ob.get("api_type", None)
    if api_type not in 'tel_basics':
        return (repeat, success, app_key, app_key_param, ts_create, ts_request, params, result, sign, interface, match,
                api_type)
    else:
        return None


sc = SparkContext(appName="data_backflow")

schema = StructType([StructField('repeat', StringType(), True), \
                     StructField('success', StringType(), True), \
                     StructField('app_key', StringType(), True), \
                     StructField('app_key_param', StringType(), True), \
                     StructField('ts_create', StringType(), True), \
                     StructField('ts_request', StringType(), True), \
                     StructField('params', MapType(StringType(), StringType(), True), True), \
                     StructField('result', StringType(), True), \
                     StructField('sign', StringType(), True), \
                     StructField('interface', StringType(), True), \
                     StructField('match', StringType(), True), \
                     StructField('api_type', StringType(), True)
                     ])

data = sc.textFile("/commit/data_backflow/api_record.json") \
    .map(lambda a: process(a)) \
    .filter(lambda a: a is not None)

sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(data, schema)

df.repartition(100).write.saveAsTable("wl_service.t_lel_test_record", mode="overwrite")

cdf = sqlContext.sql("select result from wl_service.t_lel_test_record")
cdf.select(get_json_object(regexp_replace('result','\'','\"').alias('result'),'$.data').alias("data")).filter("data is not null").show()
