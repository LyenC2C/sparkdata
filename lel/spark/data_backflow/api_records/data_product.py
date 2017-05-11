from pyspark import SparkContext
import json as json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

today = sys.argv[1]

def valid_jsontxt(content):
    if isinstance(content,unicode):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

def to_json_str_1(data):
    fields = [valid_jsontxt(i) for i in data.split("\001")]
    ob = {}
    ob.setdefault("number",fields[0])
    ob.setdefault("one",int(fields[1]))
    ob.setdefault("three",int(fields[2]))
    ob.setdefault("six",int(fields[3]))
    ob.setdefault("twelve",int(fields[4]))
    ob.setdefault("category",fields[5])
    return json.dumps(ob,ensure_ascii=False)

def to_json_str_2(data):
    fields = [valid_jsontxt(i) for i in data.split("\001")]
    ob = {}
    ob.setdefault("phone",fields[0])
    ob.setdefault("banknum",fields[1])
    ob.setdefault("idcard",fields[2])
    ob.setdefault("name",fields[3])
    ob.setdefault("time",fields[4])
    ob.setdefault("times",int(fields[5]))
    ob.setdefault("category",fields[6])
    return json.dumps(ob,ensure_ascii=False)

sc = SparkContext(appName="data_backflow_product" + today)
phone_idcard_stat =sc.textFile("/hive/warehouse/wl_service.db/t_lel_record_data_backflow_phone_idcard_res")\
        .map(lambda a:to_json_str_1(a))\
        .saveAsTextFile("/commit/data_backflow/datamart/stat_back/phone_idcard_stat_"+today)
stat_all = sc.textFile("/hive/warehouse/wl_service.db/t_lel_record_data_backflow_multifields_standard_res") \
    .map(lambda a:to_json_str_2(a)) \
    .saveAsTextFile("/commit/data_backflow/datamart/stat_back/stat_all_"+today)

