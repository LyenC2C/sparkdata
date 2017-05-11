import json as json
import datetime,time
from pyspark import SparkContext

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def process(line):
    jsonStr = valid_jsontxt(line.strip())
    ob = json.loads(jsonStr)
    api_type = ob.get("api_type","\\N")
    interface = ob.get("interface","\\N")
    api_name = ob.get("api_name","\\N")
    return "\001".join([valid_jsontxt(i) for i in [api_type,api_name,interface]])

sc = SparkContext(appName="data_backflow")
data = sc.textFile("/home/lyen/appkey.json") \
    .map(lambda a: process(a)) \
    .distinct() \
    .filter(lambda a: a is not None) \
    .saveAsTextFile("/home/lyen/appkey.parse")
    # .saveAsTextFile("/user/lel/temp/data_backflow")
def standard_params(ob):
    if ob.has_key("iname"):
        name = ob.get("iname",None)
    elif ob.has_key("name"):
        name = ob.get("name",None)
    else:
        name = None
    if ob.has_key("idCard"):
        idCard = ob.get("idCard",None)
    elif ob.has_key("cardNum"):
        idCard = ob.get("cardNum",None)
    else:
        idCard = None
    if ob.has_key("card"):
        idBank = ob.get("card",None)
    elif ob.has_key("accountNo"):
        idBank = ob.get("accountNo",None)
    elif ob.has_key("accountno"):
        idBank = ob.get("accountno",None)
    else:
        idBank = None
    if ob.has_key("mobile"):
        phone = ob.get("mobile",None)
    elif ob.has_key("phone"):
        phone = ob.get("phone",None)
    elif ob.has_key("enc_m"):
        phone = ob.get("enc_m",None)
    elif ob.has_key("ownerMobile"):
        phone = ob.get("ownerMobile",None)
    elif ob.has_key("bankpremobile"):
        phone = ob.get("bankpremobile",None)
    else:
        phone = None
    return (phone,idCard,idBank,name)



