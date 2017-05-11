import rapidjson as json
import datetime,time
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

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
    repeat = ob.get("repeat",None)
    success = ob.get("success",None)
    app_key = ob.get("app_key",None)
    create_time = ob.get("create_time",None)
    if '.' in create_time:
        create_time = create_time[:create_time.index('.')]
    ts_create = date_to_ts(create_time)
    params = ob.get("params",{})
    if params:
        sign = params.get("sign",None)
        ts_request= params.get("t",None)
        if '.' in ts_request:
            ts_request = ts_request[:ts_request.index('.')]
        app_key_param = params.get("app_key",None)
    else:
        return None
    interface = ob.get("interface",None)
    match = ob.get("match",None)
    api_type = ob.get("api_type",None)
    if api_type not in 'tel_basics':
        return  (int(repeat),int(success),long(app_key),long(app_key_param),ts_create,ts_request,params,sign,interface,float(match),api_type)

sc = SparkContext(appName="data_backflow")




schema=StructType([StructField('repeat', IntegerType(), True), \
                   StructField('success', IntegerType(), True), \
                   StructField('app_key', LongType(), True), \
                   StructField('app_key_param', LongType(), True), \
                   StructField('ts_create', StringType(), True), \
                   StructField('ts_request', StringType(), True), \
                   StructField('params', MapType(StringType(),StringType(),True), True), \
                   StructField('sign', StringType(), True), \
                   StructField('interface', StringType(), True), \
                   StructField('match', FloatType(), True), \
                   StructField('api_type', StringType(), True)
                   ])




data = sc.textFile("/user/lel/data/datamart_backflow/api_record.json") \
    .map(lambda a: process(a)) \
    .filter(lambda a: a is not None)


sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(data, schema)
'''
df.registerTempTable("table")
sqlContext.sql("use wl_service")
sqlContext.sql("insert into t_lel_test_123 select * from table")
'''
df.write.saveAsTable("wl_service.t_lel_test_123",mode="overwrite")

