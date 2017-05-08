from pyspark.sql import SQLContext
from pyspark.sql import DataFrameWriter
from pyspark.sql.types import *
from pyspark import SparkContext, HiveContext
import sys

latest = sys.argv[1]
sc = SparkContext(appName="data_backflow_standard" + latest)
sqlContext = SQLContext(sc)
hc = HiveContext(sc)
data = hc.sql(
    "select app_key_param,from_unixtime(cast(ts_request as bigint),'yyyy-MM-dd') as date,params,interface,api_type from wl_base.t_base_record_data_backflow where ds = '" + latest + "' and api_type <>'tel_basics' and not (app_key = '1186159692' and api_type not in ('tel','address_getbymobile','channel_NameIDCardAccountVerify','channel_cellphone','operator_capricorn','address_match','channel_idcard','channel_bankby3','channel_idNameFase','channel_criminal','channel_blacklistverify','credit_implement'))")


def standard_params(ob):
    if ob.has_key("iname"):
        name = ob.get("iname", None)
    elif ob.has_key("name"):
        name = ob.get("name", None)
    else:
        name = None
    if ob.has_key("idCard"):
        idCard = ob.get("idCard", None)
    elif ob.has_key("cardNum"):
        idCard = ob.get("cardNum", None)
    else:
        idCard = None
    if ob.has_key("card"):
        idBank = ob.get("card", None)
    elif ob.has_key("accountNo"):
        idBank = ob.get("accountNo", None)
    elif ob.has_key("accountno"):
        idBank = ob.get("accountno", None)
    else:
        idBank = None
    if ob.has_key("mobile"):
        phone = ob.get("mobile", None)
    elif ob.has_key("phone"):
        phone = ob.get("phone", None)
    elif ob.has_key("enc_m"):
        phone = ob.get("enc_m", None)
    elif ob.has_key("ownerMobile"):
        phone = ob.get("ownerMobile", None)
    elif ob.has_key("bankpremobile"):
        phone = ob.get("bankpremobile", None)
    else:
        phone = None
    return (phone, idCard, idBank, name)


# xiaoshudian_app_key = "1186159692"
# xiaoshudian_tec_api = ["tel","address_getbymobile","channel_NameIDCardAccountVerify","channel_cellphone","operator_capricorn","address_match","channel_idcard","channel_bankby3","channel_idNameFase","channel_criminal","channel_blacklistverify","credit_implement"]
# def filter(app_key,api):
#     not (app_key in xiaoshudian_app_key and api not in  xiaoshudian_tec_api)


data_rdd = data.rdd.map(lambda a: (a.app_key_param, a.date, standard_params(a.params), a.interface, a.api_type)) \
    .map(lambda (a, b, c, d, e): (a, b, c[0], c[1], c[2], c[3], d, e))
'''
c[0]->phone
c[1]->idcard
c[2]->idbank
c[3]->name
'''
schemaStr = "app_key date phone idcard idbank name interface api_type"
fields = [StructField(field_name, StringType(), True) for field_name in schemaStr.split()]
schema = StructType(fields)

data_df = hc.createDataFrame(data_rdd, schema).distinct()
dfw = DataFrameWriter(data_df)
dfw.saveAsTable("wl_analysis.t_lel_record_data_backflow", mode="overwrite")
