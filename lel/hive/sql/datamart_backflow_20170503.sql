datamart_backflow

geren = sqlContext.read.json("/commit/data_backflow/ge_ren_xin_xi.json")
geren_df = geren.filter("result.data is not null").select("params.enc_m","params.t")
def getTimeStamp(ts):
	return ts[0:10]
geren_df.registerTempTable("geren")
sqlContext.registerFunction("getTimeStamp",getTimeStamp)
geren_pt = sqlContext.sql("select enc_m,getTimeStamp(t) from geren where enc_m is not null").distinct()	
#phone and mobile are not isdigit,only enc_m is available here
# non-distinct  -> 199021 
# distinct      -> 199002
from pyspark.sql.functions import *	
dianshang = sqlContext.read.json("/commit/data_backflow/dianshang.json")
dianshang_pt = data.filter(get_json_object("result","$.msg") <> "成功").select("params.enc_m","params.t").distinct()

picture = sqlContext.read.json("/commit/data_backflow/jinrongyonghuhuaxiang.json")
picture_df = picture.filter("result.data is null").select("params.enc_m","params.t")
picture_df.registerTempTable("picture")
picture_pt = sqlContext.sql("select enc_m,getTimeStamp(t) from picture where enc_m is not null").distinct()	


