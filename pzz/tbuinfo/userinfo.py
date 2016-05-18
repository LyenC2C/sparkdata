#coding:utf-8
import sys,rapidjson as json
from pyspark import SparkContext

#input: line:lable \t quntitle+text
#input: feature_index_dic ({"word1":1,"word2":2,...})

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "").replace("\r", "")

def map_line(line):
    try:
        j = json.loads(valid_jsontxt(line.strip()))
        #{"alipay": "已通过支付宝实名认证", "uid": "12871760", "buycnt": "1520", "verify": "VIP等级5", "regtime": "2005.03.10", "nick": "happle_1999", "location": "北京"}
        alipay = '0' if j['alipay'] == None else '1'
        uid = j['uid']
        buycnt = j["buycnt"]
        verify = j['verify']
        regtime = j['regtime']
        nick = j['nick']
        location = j['location']

        return '\001'.join([uid,alipay,buycnt,verify,regtime,nick,location])

    except:
        pass


if __name__ == '__main__':
	#get_feature_line("0\t我爱北京天安门",{u"我":1,u"he":2,u"天":3,u"天安门":4,u"他":5})
	'''
	if sys.argv[1] == "part_1":
		sc = SparkContext(appName="pyspark portrait")
		sc.textFile(sys.argv[2])\
			.map(lambda x:get_base_info(x))\
			.saveAsTextFile(sys.argv[3])
		sc.stop()
	'''
	if sys.argv[1] == '-format':
		sc = SparkContext(appName="pyspark taobao uinfo")
		sc.textFile(sys.argv[2])\
			.map(lambda x:map_line(x))\
            .filter(lambda x:x!=None)\
			.saveAsTextFile(sys.argv[3])
		sc.stop()
