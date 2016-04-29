#coding:utf-8
import jieba,sys
from pyspark import SparkContext

#input: line:lable \t quntitle+text
#input: feature_index_dic ({"word1":1,"word2":2,...})

def valid_txt(line):
	a = ""
	u = line.strip()
	u = u.decode('UTF-8') if type(u) != type(u'') else u
	for uchar in u:
		if (uchar >= u'\u4e00' and uchar<= u'\u9fa5') or (uchar >= u'\u0030' and uchar<=u'\u0039') or (uchar >= u'\u0041' and uchar<=u'\u005a') or (uchar >= u'\u0061' and uchar<=u'\u007a') or (uchar == '\t') or (uchar == '-') or (uchar=='.'):
			a += uchar.encode('utf-8')
	return a.decode("utf-8")


def pre_pro_line(line):
	ls = line.strip().split("\",")
	part = ls[1].strip().split(",\"")
	#return part[0]+'\t'+ls[0][1:].replace("\t"," ").decode("utf-8").encode("utf-8")+'\t'+part[1][:-1].decode("utf-8").replace("\t"," ").encode("utf-8")
	try:
		return part[0]+'\t'+ls[0][1:].replace("\t"," ")+'\t'+part[1][:-1].replace("\t"," ")
	except:
		return part[0]+'\t'+ls[0][1:].replace("\t"," ")

#input: line :qun.info.format(id title text)
#return: [id,title,0,txt(title+text)]
def format_feature_input(line):
    ls = line.strip().split("\t")
    #'''
    txt = ""
    for each in ls[1:]:
        txt += each+' '
    title = ls[1] if len(ls)>1 else ""
    #return [ls[0],title,'0',valid_txt(txt).strip()]
    #'''
    #---------------delete soon
    return [ls[0],title,'0',txt.strip()]

#input: line:(0 \t txt )
#return1: [0 , '1:1:00,...' , {1:1.00,...}]
#reutrn2: None
def get_feature_line(line,feature_index_dic):
	#for each in jieba.cut("0\t我爱北京天安门"):print each
	ss = line.strip().split("\t")
	if len(ss) < 2:
		return ss[0]
	text = ss[1]
	seg_list = list(jieba.cut(text.strip().replace("\"", "")))
	out_str = ""
	index_list = []
	for word in seg_list:
		if word in feature_index_dic:
			index_list.append(feature_index_dic[word])
	index_sort = sorted(set(index_list))
	dic = {}
	for index in index_sort:
		out_str += str(index) + ":1.00 "
		dic[index] = 1.00
	if out_str == "":
		return None
	else:
		return [ss[0], out_str.strip(), dic]

#return1:None
#return2:[id,title,0,dic]
def format_to_predict(line):
	l1 = line.strip().split("\t")
	try:
		ls = l1[3].strip().split(" ")
		dic = {}
		for each in ls:
			part = each.strip().split(":")
			dic[int(part[0])] = float(part[1])
		return [l1[0],l1[1],l1[2],dic]
	except:
		return None

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
	if sys.argv[1] == "pre_pro_line":
		sc = SparkContext(appName="pyspark quninfo prepro")
		sc.textFile(sys.argv[2])\
			.map(lambda x:pre_pro_line(x))\
			.saveAsTextFile(sys.argv[3])
		sc.stop()

	# argvs:feature_index,input,output
	if sys.argv[1] == "get_feature_line":
		sc = SparkContext(appName="pyspark quninfo get_feature")
		feature_index_dic = sc.broadcast(sc.textFile(sys.argv[2]).map(lambda x:(x.strip().split("\t")[0],x.strip().split("\t")[1])).collectAsMap())
		sc.textFile(sys.argv[3])\
			.map(lambda x:format_feature_input(x))\
			.map(lambda (x,y,z):(x,y,get_feature_line(z,feature_index_dic.value)))\
			.filter(lambda (x,y,z):z!= None)\
			.map(lambda (x,y,z):x+'\t'+y+'\t'+z)\
			.saveAsTextFile(sys.argv[4])
		sc.stop()

	if sys.argv[1] == "do_predict":
		sc = SparkContext(appName="pyspark do predict")
		sc.addPyFile("/mnt/pzz/portrait/lib/liblinear_pro2.py")
		import liblinear_pro2 as func
		m = func.load_model("/mnt/pzz/portrait/lib/model")
		sc.textFile(sys.argv[2])\
			.map(lambda x:format_to_predict(x))\
			.filter(lambda x:x!=None)\
			.map(lambda (a,b,c,d):(a,b,c,func.predict_one(m,d)))\
			.map(lambda (a,b,c,d):a+'\t'+b+'\t'+c+'\t'+d[0]+'\t'+str(d[1][0]))\
			.saveAsTextFile(sys.argv[3])
		sc.stop()
		#rdd = sc.textFile("/user/hadoop/portrait/feature_format.dir/part-*").map(lambda x:predict_pro(x,m))

	#input:
	#argv[2]: app name
	#argv[3]: feature_local_file
	#argv[4]: quninfo.format.dir/part*(id \t title \t text)
	#argv[5]: feature out put file
	#argv[6]: model_local_file
	#argv[7]: predict out put file
	#argv[8]: user_dict
	if sys.argv[1] == "feature_and_predict":

		def get_feature_index_dic(feature_file):
			dic = {}
			n = 1
			for line in open(feature_file,"r"):
				dic[line.strip().decode("utf-8")] = n
				n += 1
			return dic

		sc = SparkContext(appName="pyspark "+sys.argv[2])
		feature_index_dic = sc.broadcast(get_feature_index_dic(sys.argv[3]))
		jieba.load_userdict(sys.argv[8])
		
		rdd_f = sc.textFile(sys.argv[4])
		rdd_1 = rdd_f.map(lambda x:format_feature_input(x))\
			.map(lambda (a,b,c,d):(a,b,d,get_feature_line(c+'\t'+d,feature_index_dic.value)))\
			.filter(lambda (a,b,c,d):d!=None and len(d)==3)\
			.cache()
		#rdd_1.map(lambda (id,title,(flag,x_str,x_dic)):id+'\t'+title+'\t'+flag+'\t'+x_str)\
		rdd_1.map(lambda (a,b,c,d):a+'\t'+b+'\t'+d[0]+'\t'+d[1]+'\t'+c)\
			.saveAsTextFile(sys.argv[5])

		sc.addPyFile("/mnt/pzz/portrait/lib/liblinear_pro2.py")
		import liblinear_pro2 as func
		m = func.load_model(sys.argv[6])
		rdd_1.map(lambda (a,b,c,v):(a,b,v[0],func.predict_one(m,v[2]),c))\
			.map(lambda (a,b,v1,v2,c):a+'\t'+b+'\t'+v1+'\t'+v2[0]+'\t'+str(v2[1][0])+'\t'+c)\
			.saveAsTextFile(sys.argv[7])
