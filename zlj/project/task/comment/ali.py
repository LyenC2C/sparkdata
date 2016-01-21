#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
import time
import rapidjson as json

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

# -*- coding: utf-8 -*-
import sys, os
import re
ROOTDIR = os.path.join(os.path.dirname(__file__), os.pardir)
sys.path.append(os.path.join(ROOTDIR, "lib"))
sys.path.append('/usr/local/lib/python2.7/dist-packages')

# Set your own model path
MODELDIR=os.path.join(ROOTDIR, "ltp_data")


MODELDIR='/home/hadoop/pyltp/tlpdata/ltp_data/'
print ROOTDIR   ,MODELDIR
from pyltp import Segmentor, Postagger, Parser, NamedEntityRecognizer, SementicRoleLabeller

import sys

reload(sys)
sys.setdefaultencoding('utf8')

# import time
# def time_me(fn):
#     def _wrapper(*args, **kwargs):
#         start = time.clock()
#         fn(*args, **kwargs)
#         print "%s cost %s second"%(fn.__name__, time.clock() - start)
#     return _wrapper

#sentence = "中国进出口银行与中国银行加强合作"
#sentence ="努西娜 2015秋冬平底短靴女真皮厚底靴子马丁靴平跟女靴切尔西靴"
#sentence ="春秋韩国百搭围巾冬季两用超长款女士纱巾秋冬棉麻披肩渐变色丝巾"
#sentence ="龙之谷游戏账号80级雷神70A新月套华南一区2.5W攻击带时装坐骑甩"
#sentence =sys.argv[1]

f_degree = open('degree').readlines()
f_negative = open('negative')
degree_dict = {}
negative_dict = {}
for i in range(len(f_degree)):
    word = f_degree[i].strip().split('\001')[0]
    if i < 13:
        degree_dict[word] = 2
    if i >= 13 and i < 29:
        degree_dict[word] = 1
    if i >= 29:
        degree_dict[word] = 3
for line in f_negative:
    word = line.strip().split('\001')[0]
    negative_dict[word] = -1

segmentor = Segmentor()
segmentor.load(os.path.join(MODELDIR, "cws.model"))
postagger = Postagger()
postagger.load(os.path.join(MODELDIR, "pos.model"))
parser = Parser()
parser.load(os.path.join(MODELDIR, "parser.model"))
def parse(words_tags):
    #words = segmentor.segment(sentence)
    #words="清洁 不是 很 彻底   感觉 不是 正品".split()
    #print words
    #postags = postagger.postag(words)
    words = words_tags[0]
    postags = words_tags[1]
    mp={}
    for index, i in enumerate(postags,1):mp[index]=i
    mp[0]='HEAD'
    arcs = parser.parse(words, postags)
    m={}
    for index, i in enumerate(words,1):m[index]=i
    m[0]='HEAD'
    #print "\t".join("%d:%s" % (arc.head, arc.relation) for index,arc in enumerate(arcs)
    ls=[]
    for index,arc in enumerate(arcs,1):
        index2 = arc.head
        ls.append((index,m[index],mp[index],index2,m[arc.head],mp[arc.head],arc.relation))
    return ls

vs=['有','没有','是','看','不是']
COMMON=[",",".","!","~",";",":","?","，","。","！","～","；","：","？","…","—","-","\t"]

window=5
clauses_limit = 15

pair_1=['nv','np','vn','vd','rv','jv','vv','nlv','wsv','vr','qv','av','nda','ra','ar','nc','cn']
pair_2=['nv','vn','nd','dn','ad','da','ndv','vnd','nhv','vnh','nc','cn']
pair_vob = ['ev']
people = ["宝宝","妈","爸","爷","老爹","我爹","奶奶","我奶","舅","外甥","叔","父","母亲","婶","姨","儿子","女儿","女婿","公公","婆","姥","哥","弟","姐","妹","老公","丈夫","妻子","媳妇","朋友","孙子"]

tag_filter_set=set('u   o   x   ws  wp  q   e   g'.split())

word_filter_set=set(''.split())

re_chinese = re.compile(u'[^a-zA-Z0-9\u4e00-\u9fa5]+')

def extract(ln):
    sen_de_neg = []
    words =re_chinese.sub(' ', ln.decode('utf-8').rstrip()).encode('utf-8').split()
    neg = 1
    degree = 0
    for word in words:
        if negative_dict.has_key(word):
            neg = neg * (-1)
        if degree_dict.has_key(word):
            degree = max(degree,degree_dict[word])

    #while '' in words:
    #    words.remove('')
    postags = postagger.postag([i.strip() for i in words if len(i)>0])
    bl = 1
    for tag in postags:
        if tag[0] == 'n': #首字母为n的名词
            bl = 0
            break
    if bl == 1:
        words = ["商品"] + list(words)
        postags = ["n"] + list(postags)
    words_l=[]
    postags_l=[]
    for word ,tag  in zip(words,postags):
        if tag=='r': continue # 代词过滤
        if tag not  in tag_filter_set: #过滤其他不重要词性
            words_l.append(word)
            postags_l.append(tag)
    cla_words_tags = (words_l,postags_l)
    caluses = "\002".join(cla_words_tags[0])
    sen_de_neg.append(caluses)
    sen_de_neg.append(neg)
    sen_de_neg.append(degree)
    if len(cla_words_tags[0]) > clauses_limit: return
    for ln in  people:
        if ln in caluses:
            return
    ls = parse(cla_words_tags)
    for index,i in enumerate(ls,1):
        if i[6]=='SBV':
            if i[2]+i[5] not in pair_1:
                sen_de_neg.append(str(i[1])+":"+str(i[4]))
                #print ' '.join([str(j) for j in i])
        if i[6]=='VOB':#n v n
            index,w1,w1p,index2,w2,w2p,r=i
            if (w2 in vs) and w1p+w2p not in pair_vob:
                for j  in xrange(window):
                    bound = index-2-j
                    if bound < 0: break
                    index_t,w1_t,w1p_t,index2_t,w2_t,w2p_t,r_t = ls[index-2-j]
                    if w1p_t+w2p_t in pair_2 and (index_t == index2 or index2_t == index2):
                        if w1p_t[0] == 'n':
                            sen_de_neg.append(w1_t + ":"+ w1)
                            #print index_t,w1_t,w1p_t,index2_t,w2_t,w2p_t,index,w1,w1p,'VOB'
                        if w2p_t[0] == 'n':
                            sen_de_neg.append(w2_t + ":" + w1)
                            #print w2_t,w2p_t,w1_t,w1p_t,w1,w1p,'VOB'
    return ",".join([str(j) for j in sen_de_neg])
    #for ln in ls:
    #    print ' '.join([str(j) for j in ln])
#@time_me
fuhao = '[,.!~;:?，。！～；：？…— \t]'.decode('utf-8')
fuhao_s= '[,.!~;:?，。！～；：？…— \t]'.decode('utf-8')
import re
def fenju(line):
    sentence = line.strip()
    # for i in COMMON:
    #     sentence = sentence.replace(i," ")
    sentence = re.sub(fuhao,' ',sentence.decode('utf-8')).encode('utf-8')
    ss = sentence.split("   ")
    #ss = re.split('[,.!~;:?，。！～；：？…— \t]', line.strip())
    #sss = []
    result = []
    for ln in ss:
        result.append(extract(ln))
    return result

def  parse_line(line):
    ss = line.strip().split('\001')
    if len(ss) != 4 :return ""
    base_info = []
    fenju_list = []
    base_info.append(ss[0]) #item_id
    #feed_id = ss[1] #feed_id
    base_info.append(ss[1]) #feed_id
    base_info.append(ss[2]) #user_id
    base_info.append(ss[3].strip().replace(' ','\002') )#content
    content = ss[3]
    #print ss[1] + "\t" + content.strip().replace(" ","")
    #print "\t".join(base_info)
    string = ""
    string += "\t".join(base_info) + "\t"
    #f_out.write("\t".join(base_info) + "\t")
    fenju_list = fenju(content)
    #print "|".join([i for i in fenju_list  if i is not None])
    #f_out.write("|".join([i for i in fenju_list  if i is not None]) + "\n")
    # string += "|".join([i for i in fenju_list  if i is not None]) + "\n"
    string += "|".join([i for i in fenju_list  if i is not None])
    return string
#@time_me
def main():
    path = sys.argv[1]
    name = path.split('/')[-1]
    f_out = open('parse_'+name, 'w')
    string = ""
    list_string = []
    yuzhi = 50000

    count = 0
    # fr=open(path)
    with open(path, 'rb') as fr:
        for line in fr:
            count += 1
            #print i,
            #line = f_in[i]
            try:
                list_string.append(parse_line(line))
            except Exception, e:
                pass
            # list_string.append(line)

            if count % yuzhi == 0 and len(list_string)>0:
                f_out.write('\n'.join(list_string))
                # string = ""
                list_string=[]

    # for line in open(path).readlines():
    #     #print i
    #     count += 1
    #     #print i,
    #     #line = f_in[i]
    #     list_string.append(parse_line(line))
    #     if count % yuzhi == 0:
    #         f_out.write(string)
    #         string = ""
    if len(list_string)>0:
        f_out.write('\n'.join(list_string))

main()


    # clauses = fenju(content)
    # #print line
    # for cla in clauses:
    #     #if len(cla[0].decode("utf-8")) > clauses_limit: continue
    #     extract(cla)
    #print "------------------------------"
    #f_out.write("|".join(fenju_list))
    #f_out.write('\n')
    # print

    #for index,i in enumerate(ls,1):
    #    print ' '.join([str(j) for j in i])

#recognizer = NamedEntityRecognizer()
#recognizer.load(os.path.join(MODELDIR, "ner.model"))
#netags = recognizer.recognize(words, postags)
#print 'name'
#print "\t".join(netags)

#labeller = SementicRoleLabeller()
#labeller.load(os.path.join(MODELDIR, "srl/"))
#labeller.load("/home/yjliu/ltp/model/srl/")
#roles = labeller.label(words, postags, netags, arcs)

#for role in roles:
#    print role.index, "".join(
#            ["%s:(%d,%d)" % (arg.name, arg.range.start, arg.range.end) for arg in role.arguments])
