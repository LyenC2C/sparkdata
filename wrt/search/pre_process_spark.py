#coding:utf-8
import sys
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
    if len(ls) < 2:
        pass
    else:
        #cate=ls[-1].split('$')[0]
        #if cat_name == cate:
        return (ls[-2].replace('$','|'),ls[-1].replace('$','|'))
    #else:
    #	return None
###获取固定格式的目标item.data数据
def get_item(cat_dict, line):
    lv = []
    ls = line.strip().split('\t')
    if len(ls) < 24:
        return None
    key = ls[14].replace('-', '|')
    import re
    if re.findall(r'\d+',key):
        key='|'.join(key.split('|')[:-2])
    if key in cat_dict:
        url_key = ls[3].split('/')[-1].split('=')[-1].split('?')[0].split('.')[0]#url规范化方法
        flag = "1"  #item表的flag为1
        lv.append(valid_jsontxt(ls[2]))
        lv.append(valid_jsontxt(ls[14]))
        lv.append(valid_jsontxt(cat_dict[key]))
        lv.append(valid_jsontxt(ls[15]))
        lv.append(flag)
        return (valid_jsontxt(url_key), "\t".join(lv))
        #匹配关键词, 商品title 类目 标准类目 品牌
    else:
        return None
###存储规范化的item_url映射关系
'''
def get_item_dict(line):
    ls = line.strip().split('\t')
    key = ls[3].split('/')[-1].split('=')[-1].split('?')[0].split('.')[0]#url规范化方法
    return (key,"\t".join((ls[2],ls[4],ls[6],ls[5]))) #商品title 类目 品牌 标准类目
'''
##提取搜索行为
def get_sousuo(ss):
    p = False
    srch_url = None
    if "query=" in ss:
        p = True
        srch_url = ss.split('query=')[-1]
    if "q=" in ss:
        p = True
        srch_url = ss.split('q=')[-1]
    if ("word=" in ss) and not ("sword" in ss):
        p = True
        srch_url = ss.split('word=')[-1]
    if "wd=" in ss:
        p = True
        srch_url = ss.split('wd=')[-1]
    if "keyword=" in ss:
        p = True
        srch_url = ss.split('keyword=')[-1]
    if "keyWord=" in ss:
        p = True
        srch_url = ss.split('keyWord=')[-1]
    if p:
        return (True, srch_url)
    else:
        return (False, srch_url)
#获取有效用户行为
def get_pageview(line):
    ls = line.strip().split('\t')
    lv = []
    if len(ls) < 13:
        return None
    if ls[6] == ls[11] and len(ls[12]) > 0 and ls[12] != "NULL" and len(ls[13]) > 0 and get_sousuo(ls[12])[0]:
        url_key = ls[11].split('/')[-1].split('=')[-1].split('?')[0].split('.')[0]
        srch_word = ls[13]
        if ls[13] == "NULL":
            srch_url = valid_jsontxt(get_sousuo(ls[12])[1]).split('&')[0]
            srch_word = urllib.unquote(srch_url) #+ "***"
        #if valid_jsontxt(srch_word).isdigit(): #去掉纯数字
            #return None
        flag = "2"  #用户行为表的flag为2
        lv.append(valid_jsontxt(srch_word))
        lv.append(valid_jsontxt(ls[0]))
        lv.append(valid_jsontxt(ls[11]))
        lv.append(valid_jsontxt(ls[12]))
        lv.append(flag)
        return (valid_jsontxt(url_key),'\t'.join(lv))
        #匹配关键词 , 搜索词 网站名 商品页url 搜索引擎页url
    else:
        return None
#用户行为表与item表关联成一张表，x为关联的key，y为集成了flag=1与2的列表
def heti(x,y):
    lv = []
    v1 = "0"
    v2 = "0"
    y = list(set(y))   #将列表去重
    for i in range(len(y)):
        ss = y[i].split('\t')
        if ss[4] == '1':
            v1 = y[i]
            #y.remove(y[i])
    #将v1的值提出来，并且只保留一个值
    if v1 == "0":
        return [None]
    for ln in y:
        ss = ln.split('\t')
        if ss[4] == '2':
            v2 = ln
            lv.append("\t".join((v2, v1, x)))
    #把每个v2与之前的v1和x一起作为结果
    return lv

if __name__ == "__main__":
    sc=SparkContext(appName="pyspark baifendian pre_process")
    #cat_name=sys.argv[1]
    #cat_name=u"日用百货"
    #广播字典
    cat_dict = sc.broadcast(sc.textFile(sys.argv[1]+"*").map(lambda x: get_cat_map(x)).filter(lambda x:x!=None)\
        .repartition(1).collectAsMap())
    #item.data运算
    rdd_item = sc.textFile(sys.argv[2]).map(lambda x: get_item(cat_dict.value, x)).filter(lambda x:x!=None)
        #.groupByKey().mapValues(list).map(lambda (x, y): (x, y[0]))
    #广播item_dict
    #item_dict = sc.broadcast(rdd_item.map(lambda x:get_item_dict(x)).filter(lambda x:x!=None).collectAsMap())
    #pageview.data运算
    rdd_pageview = sc.textFile(sys.argv[3]).map(lambda x:get_pageview(x)).filter(lambda x:x!=None)
        #.groupByKey().mapValues(list).map(lambda (x,y): (x,y[0]))
    #rdd_item.saveAsTextFile(sys.argv[4])
    rdd = rdd_pageview.union(rdd_item).groupByKey().mapValues(list).flatMap(lambda (x, y): heti(x, y)).filter(lambda x:x!=None)
    rdd.saveAsTextFile(sys.argv[4])
    sc.stop()
    #for line in sys.stdin:
    #print map(lambda line:get_cat_map("日用百货",line),[line for line in sys.stdin])

# spark-submit  --executor-memory 4G  --driver-memory 10G  --total-executor-cores 40 pre_process_spark.py \
# /user/wrt/cat_map.txt /user/wrt/test/item_家居家纺.dat /user/wrt/test/pageview_家居家纺.dat \
# /user/zlj/wrt/test/jiaju_keyword_itemtitle7

