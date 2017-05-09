import rapidjson as json
import datetime,time
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext(appName="income_analysis")
sqlContext = SQLContext(sc)

geren = sqlContext.read.json("/commit/data_backflow/ge_ren_xin_xi.json")
geren_df = geren.filter("result.data is not null").select("params.enc_m","params.t")
geren_df.registerTempTable("geren")
sqlContext.registerFunction("getTimeStamp",lambda a:a[0:10])
geren_pt = sqlContext.sql("select enc_m,getTimeStamp(t) as t from geren where enc_m is not null")
geren_pt.distinct().count()
geren_pt.count()
geren_df_n = geren.filter("result.data is null").select("params.enc_m","params.t")
geren_df_n.registerTempTable("geren_n")
sqlContext.registerFunction("getTimeStamp",lambda a:a[0:10])
geren_pt_n= sqlContext.sql("select enc_m,getTimeStamp(t) as t from geren_n where enc_m is not null")


dianshang = sqlContext.read.json("/commit/data_backflow/dianshang.json")
dianshang_pt = dianshang.filter(get_json_object("result","$.msg") <> "成功").select("params.enc_m","params.t")
dianshang_pt.distinct().count()
dianshang_pt.count()
dianshang_pt_y = dianshang.filter(get_json_object("result","$.msg") == "成功").select("params.enc_m","params.t")
dianshang_pt_y.distinct().count()
dianshang_pt_y.count()
'''
#read.json速度贼慢,所以还是采用rapidjson解析json字符串
picture = sqlContext.read.json("/commit/data_backflow/jinrongyonghuhuaxiang.json")
picture_df = picture.filter("result.data is null").select("params.enc_m","params.t")
picture_df.registerTempTable("picture")
picture_pt = sqlContext.sql("select enc_m,getTimeStamp(t) from picture where enc_m is not null").distinct()
'''

# geren_pt.join(dianshang_pt,geren_df.enc_m=dianshang_pt.enc_m)

picture = sc.textFile("/commit/data_backflow/jinrongyonghuhuaxiang.json")

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

def process(js):
    ob = json.loads(valid_jsontxt(js))
    if not isinstance(ob,dict):return None
    result = ob.get("result",{})
    if not isinstance(result,dict):return None
    data = result.get("data",None)
    phone = ob.get("params",{}).get("enc_m",None)
    ts = ob.get("params",None).get("t",None)[0:10]
    if data is not None:
        return (phone,ts,'1')
    else:
        return (phone,ts,'0')


picture_ptt = picture.map(lambda a:process(a)).filter(lambda a:a is not None).cache()
picture_y = picture_ptt.filter(lambda a:a[2] =='1').map(lambda a:(a[0],a[1]))
picture_n = picture_ptt.filter(lambda a:a[2] =='0').map(lambda a:(a[0],a[1]))

geren_pt.map(lambda a:(a.enc_m,a.t)).distinct().join(picture_y.distinct()).filter(lambda (a,(b,c)):(int(b)-int(c))<=5).count()
#y->1169
geren_pt.map(lambda a:(a.enc_m,a.t)).distinct().join(picture_n.distinct()).filter(lambda (a,(b,c)):(int(b)-int(c))<=5).count()
#n->2540


geren_pt.map(lambda a:(a.enc_m,a.t)).distinct().join(dianshang_pt_y.map(lambda a:(a.enc_m,a.t)).distinct()).filter(lambda (a,(b,c)):(int(b)-int(c))<=5).count()
#y->35856

geren_pt.map(lambda a:(a.enc_m,a.t)).distinct().join(dianshang_pt.map(lambda a:(a.enc_m,a.t)).distinct()).filter(lambda (a,(b,c)):(int(b)-int(c))<=5).count()
#n->286034

geren_pt.map(lambda a:(a.enc_m,a.t)).join(picture_n)
