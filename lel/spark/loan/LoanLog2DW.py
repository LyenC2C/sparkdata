# coding=utf-8
import rapidjson as json
from pyspark.sql import SparkSession
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
table = sys.argv[1]
last_day = sys.argv[2]

spark = SparkSession \
    .builder \
    .appName("loan" + last_day) \
    .master("yarn-client") \
    .enableHiveSupport() \
    .getOrCreate()
# .master("spark://master:7077") \
# .config("hive.metastore.warehouse.dir", "hdfs://cs11:9600/hive/warehouse") \

FRAUD_FOCUS_LIST = "欺诈关注清单"
FRAUD_APPLY_SCORE = "欺诈申请分"
FRAUD_APPLY_VERIFY = "欺诈申请验证"
ZM_CREDIT_SCORE = "芝麻信用分"
BUSINESS_FOCUS_LIST = "行业关注名单"

loan_path = "hdfs://cs11:9600/commit/loan/" + last_day+"/*"
# loan_path = "hdfs://master:9000/data/wolong/loan"

original_data = spark.sparkContext.textFile(loan_path)


def valid_jsontxt(content):
    if isinstance(content, unicode):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def get_records(record):
    fields = [valid_jsontxt(i) for i in record.split("\t")]
    idCard = fields[0]
    name = fields[1]
    phone = fields[2]
    resTime = fields[4]
    type = fields[5]
    result = fields[3]
    ob = json.loads(result)
    data_ob = ob.get("data")
    data = None
    if type in FRAUD_FOCUS_LIST:
        data = None if data_ob.get("hit") in "no" else data_ob.get("risk_code")
    elif type in FRAUD_APPLY_SCORE:
        data = data_ob.get("score")
    elif type in FRAUD_APPLY_VERIFY:
        data = data_ob.get("verify_code")
    elif type in ZM_CREDIT_SCORE:
        state = data_ob.get("state")
        zm_score = data_ob.get("zm_score")
        data = dict({"state": state}.items() + {"zm_score": zm_score}.items())
    elif type in BUSINESS_FOCUS_LIST:
        if data_ob.get("is_matched"):
            details = data_ob.get("details")
            for detail in details:
                detail.pop("level")
                if detail.has_key("settlement"):
                    detail.pop("settlement")
            data = details
        else:
            data = None
    typeTransformed = None
    if type in FRAUD_FOCUS_LIST:
        typeTransformed = "fraud_focus_list"
    elif type in FRAUD_APPLY_SCORE:
        typeTransformed = "fraud_apply_score"
    elif type in FRAUD_APPLY_VERIFY:
        typeTransformed = "fraud_apply_verify"
    elif type in ZM_CREDIT_SCORE:
        typeTransformed = "zm_credit_score"
    else:
        typeTransformed = "business_focus_list"
    return (json.dumps({"idCard": idCard, "name": name, "phone": phone, "reqTime": resTime}), {typeTransformed: data})


'''
records = original_data.map(lambda a: get_records(a)).reduceByKey(
    lambda a, b: dict(a.items() + b.items())).map(
    lambda (a, b): json.dumps(dict
                              (json.loads(a).items() + {"various_records": b}.items())))
'''
records = original_data.map(lambda a: get_records(a)).reduceByKey(lambda a, b: dict(a.items() + b.items())).map(
    lambda (a, b): json.dumps(dict
                              (json.loads(a).items() + b.items() + {"ds": last_day}.items())))

records_df = spark.read.json(records)
records_df.coalesce(1).write.saveAsTable(name=table, partitionBy="ds", mode="append")
'''默认保存文件为nappy压缩的parquet文件,用Hive分析需注意复杂类型字段'''
