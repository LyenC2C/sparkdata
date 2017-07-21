import rapidjson as json
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import datetime
from pyspark.rdd import ignore_unicode_prefix

sc = SparkContext(appName="income_analysis")
sqlContext = SQLContext(sc)

@ignore_unicode_prefix
def timestamp2string(timeStamp):
    try:
        d = datetime.datetime.fromtimestamp(timeStamp)
        str1 = d.strftime("%Y%m")
        return str1
    except Exception as e:
        print e
        return ''

'''
9438780765
'''
geren = sqlContext.read.json("/commit/data_backflow/ge_ren_xin_xi.json")
geren_df = geren.filter("result.data is not null")\
                  .select(geren.app_key,geren.params.enc_m.alias("enc_m"),geren.params.t[0:10].alias("t"))\
                  .filter("cast(t as int) > 1485878400")



ec = sqlContext.read.json("/commit/data_backflow/dianshang.json")
ec_df = ec.filter(get_json_object("result","$.data").isNotNull())\
          .select("params.app_key","params.enc_m",ec.params.t[0:10].alias("t"))\
          .filter("cast(t as int)> 1485878400")
ec_stat_self = ec_df.map(lambda a:((a.app_key,timestamp2string(float(a.t))),1)).reduceByKey(lambda a,b:a+b)


pic = sqlContext.read.json("/commit/data_backflow/yonghuhuaxiang.json")
pic_df = pic.filter(get_json_object("result","$.data").isNotNull()) \
    .select("params.app_key","params.enc_m",pic.params.t[0:10].alias("t")) \
    .filter("cast(t as int)> 1485878400")

pic_stat_self = pic_df.map(lambda a:((a.app_key,timestamp2string(float(a.t))),1)).reduceByKey(lambda a,b:a+b)



'''
ps:
jinrongyonghuhuaxiang.json金融用户画像数据
result中字段很多,json化很慢
read.json速度贼慢,所以还是采用rapidjson解析json字符串
'''

# geren_pt.join(dianshang_pt,geren_df.enc_m=dianshang_pt.enc_m)

jr_picture = sc.textFile("/commit/data_backflow/jinrongyonghuhuaxiang.json")

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
    app_key = ob.get("app_key",None)
    phone = ob.get("params",{}).get("enc_m",None)
    ts = ob.get("params",None).get("t",None)[0:10]
    if int(ts) > 1485878400:
        if data is not None:
            return (app_key,phone,ts,'1')
        else:
             return (app_key,phone,ts,'0')
    else:
        return None
'''
['3926502285', '1186159692', '6242378468', '6424773687', '1090755074', '9438780765', '7346285482', '0982419639', '1066824805', '4058939122']
'''
jr_picture_y = jr_picture.map(lambda a:process(a))\
                .filter(lambda a:a is not None)\
                .filter(lambda a:a[3] =='1').map(lambda a:(a[1],(a[2],a[0])))
jr_stat_self = jr_picture_y.map(lambda a:((a[1][1],timestamp2string(float(a[1][0]))),1)).reduceByKey(lambda a,b:a+b)



#(geren.enc_m,(geren.t,(jr.t,jr.app_key)))
jr_picture_data = geren_df.map(lambda a:(a.enc_m,a.t)).join(jr_picture_y).filter(lambda (a,(b,(c,d))):(int(b)-int(c))<=6)
jr_stat = jr_picture_data.map(lambda (a,(b,(c,d))):((d,timestamp2string(long(b))),1)).reduceByKey(lambda a,b:a+b)


ec_data = geren_df.map(lambda a:(a.enc_m,a.t)).join(ec_df.map(lambda a:(a.enc_m,(a.t,a.app_key)))).filter(lambda (a,(b,(c,d))):(int(b)-int(c))<=6)
ec_stat = ec_data.map(lambda (a,(b,(c,d))):((d,timestamp2string(long(b))),1)).reduceByKey(lambda a,b:a+b)


pic_data = geren_df.map(lambda a:(a.enc_m,a.t)).join(pic_df.map(lambda a:(a.enc_m,(a.t,a.app_key)))).filter(lambda (a,(b,(c,d))):(int(b)-int(c))<=6)
pic_stat = pic_data.map(lambda (a,(b,(c,d))):((d,timestamp2string(long(b))),1)).reduceByKey(lambda a,b:a+b)


def fillna(data):
    return 0 if data is None  else data


user = sqlContext.read.json("/commit/data_backflow/user.json").map(lambda a:(a.app_key,a.companyname))

jr_res= jr_stat_self.leftOuterJoin(jr_stat).map(lambda ((a,b),(c,d)): (a,(b,c-fillna(d),fillna(d))))\
        .join(user)\
        .map(lambda (a,((b,c,d),e)): "\001".join([valid_jsontxt(i) for i in [e,b,c,d]]))
jr_res.coalesce(1).saveAsTextFile("/user/lel/temp/test_analysis_jryhhx")

ec_res = ec_stat_self.leftOuterJoin(ec_stat).map(lambda ((a,b),(c,d)): (a,(b,c-fillna(d),fillna(d)))).join(user) \
    .map(lambda (a,((b,c,d),e)): "\001".join([valid_jsontxt(i) for i in [e,b,c,d]]))
ec_res.coalesce(1).saveAsTextFile("/user/lel/temp/test_analysis_ec")



