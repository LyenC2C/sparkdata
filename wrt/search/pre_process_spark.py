#coding:utf-8
import sys
#import chardet
import urllib
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkContext
def f_coding(x):
    if type(x) == type(""):
        return x.decode("utf-8")
    else:
        return x
def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content
##读取品类映射表，作为过滤条件
def get_cat_map(line):
    ls=line.strip().split('\t')
    #print ls
    if len(ls)<2:
        pass
    else:
        #cate=ls[-1].split('$')[0]
        #if cat_name ==cate:
        return (ls[-2].replace('$','|'),ls[-1].replace('$','|'))
    #else:
    #	return None
###获取固定格式的目标item.data数据
def get_item(cat_dict,line):
    ls=line.strip().split('\t')
    if len(ls)<24:
        return None
    key=ls[14].replace('-', '|')
    import re
    if re.findall(r'\d+',key):
        key='|'.join(key.split('|')[:-2])
    if key in cat_dict:
        return '\t'.join((ls[0],ls[1],ls[2],ls[3],ls[14],ls[15],cat_dict[key]))
        #网站英文id 商品id 商品title 商品url 类目 品牌 标准类目
    else:
        return None
###存储规范化的item_url映射关系
def get_item_dict(line):
    ls = line.strip().split('\t')
    key = ls[3].split('/')[-1].split('=')[-1].split('?')[0].split('.')[0]#url规范化方法
    return (key,"\t".join((ls[2],ls[4],ls[5],ls[6])))
##提取搜索行为
def get_sousuo(ss):
    p = False
    srch_word = None
    if "query=" in ss:
        p = True
        srch_word = ss.split('query=')[-1]
    if "q=" in ss:
        p = True
        srch_word = ss.split('q=')[-1]
    if "word=" in ss:
        p = True
        srch_word = ss.split('word=')[-1]
    if "wd=" in ss:
        p = True
        srch_word = ss.split('wd=')[-1]
    if "keyword=" in ss:
        p = True
        srch_word = ss.split('keyword=')[-1]
    if "keyWord=" in ss:
        p = True
        srch_word = ss.split('keyWord=')[-1]
    #if srch_word != None:
        #srch_word = urllib.unquote(srch_word)
    if p:
        #print "\t".join(ss)
        return (True,srch_word)
    else:
        return (False,srch_word)
#获取有效用户行为
def get_pageview(item_dict,line):
    ls = line.strip().split('\t')
    if len(ls) < 13:
        return
    if ls[6] == ls[11] and len(ls[12])>0 and ls[12] != "NULL" and len(ls[13]) > 0:
        if get_sousuo(ls[12])[0]:
            key=ls[11].split('/')[-1].split('=')[-1].split('?')[0].split('.')[0]
            if key in item_dict:
                srch_word = ls[13]
                if ls[13] == "NULL":
                    srch_url = get_sousuo(ls[12])[1]
                    srch_word = "|*" + urllib.unquote(str(srch_url)).decode('utf-8') + "*|"
                    #srch_word += str(chardet.detect(srch_word))
                return "\t".join((srch_word, item_dict[key], ls[0], ls[11], ls[12], key))
                #搜索词
                #return "\t".join((ls[0], ls[2], ls[3], ls[11], ls[12], ls[13]))
                #网站英文id cookie userid 跳转后网站 跳转前搜索页 搜索词

if __name__ == "__main__":
    sc=SparkContext(appName="pyspark baifendian pre_process")
    #cat_name=sys.argv[1]
    #cat_name=u"日用百货"
    #广播字典
    cat_dict = sc.broadcast(sc.textFile(sys.argv[1]+"*").map(lambda x: get_cat_map(x)).filter(lambda x:x!=None).collectAsMap())
    #item.data运算
    rdd_item = sc.textFile(sys.argv[2]).map(lambda x: get_item(cat_dict.value, x)).filter(lambda x:x!=None)\
        .map(lambda x:(x,0)).groupByKey().mapValues(list).map(lambda (x,y): x)
    #广播item_dict
    item_dict = sc.broadcast(rdd_item.map(lambda x:get_item_dict(x)).filter(lambda x:x!=None).collectAsMap())
    #pageview.data运算
    rdd_pageview = sc.textFile(sys.argv[3]).map(lambda x:get_pageview(item_dict.value,x)).filter(lambda x:x!=None)\
        .map(lambda x:(x,0)).groupByKey().mapValues(list).map(lambda (x,y): x)
    #rdd_item.saveAsTextFile(sys.argv[4])
    rdd_pageview.saveAsTextFile(sys.argv[4])
    sc.stop()
    #for line in sys.stdin:
    #print map(lambda line:get_cat_map("日用百货",line),[line for line in sys.stdin])

#spark-submit  --executor-memory 4G  --driver-memory 8G  --total-executor-cores 40 pre_process_spark.py \
#/user/wrt/cat_map/ item_电器.dat  /user/zlj/baifendian.data/pageview_refer.data/pageview_Cjumeiyoupin.dat
