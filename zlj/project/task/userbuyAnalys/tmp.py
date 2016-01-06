# coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

# from pyspark import SparkContext
# from pyspark.sql import *
# from pyspark.sql.types import *
# import time
# import rapidjson as json
#
# sc = SparkContext(appName="cmt")
# sqlContext = SQLContext(sc)
# hiveContext = HiveContext(sc)
segmentor = Segmentor()
segmentor.load(os.path.join(MODELDIR, "cws.model"))
postagger = Postagger()
postagger.load(os.path.join(MODELDIR, "pos.model"))
parser = Parser()
parser.load(os.path.join(MODELDIR, "parser.model"))
def fun(line):
    words = line.split()
    print "\t".join("%d:%s"%(index,i) for index, i in enumerate(words,1))
    postags = postagger.postag(words)
    # list-of-string parameter is support in 0.1.5
    #postags = postagger.postag(["中国","进出口","银行","与","中国银行","加强","合作"])
    print "\t".join(postags)
    mp={}
    for index, i in enumerate(postags,1):mp[index]=i
    mp[0]='HEAD'
    arcs = parser.parse(words, postags)
    m={}
    for index, i in enumerate(words,1):m[index]=i
    m[0]='HEAD'
    #print "\t".join("%d:%s" % (arc.head, arc.relation) for index,arc in enumerate(arcs)
    for index,arc in enumerate(arcs,1):
        print index,m[index],mp[index], m[arc.head],mp[arc.head],arc.relation


# for line in open(''):


for line in open('E:\work\\tag'):
    vs = line.split()
    id = vs[0]
    if len(vs) == 1: continue
    # for i in vs[1:]:
    print "'" + id + "':'" + '\t'.join(vs[1:]) + "',"

# a = {
#     '50008141': '爱酒',
#     '50002766': '吃货'
# }

a = {
    '50018222': '理工男	数码发烧友	数码控',
    '50007218': '行政	office',
    '50012082': '爱下厨',
    '124044001': '数码控',
    '50022703': '数码控',
    '50008097': '数码控',
    '50019780': '数码控',
    '50011972': '数码控',
    '1512': '数码控',
    '14': '文青	爱摄影',
    '124242008': '数码发烧友	数码控',
    '50018004': '学习',
    '20': '游戏达人',
    '11': '数码控',
    '1101': '数码控',
    '33': '爱读书',
    '34': '爱音乐	爱生活',
    '50017300': '音乐	品质',
    '50017908': '彩迷',
    '50023722': '近视	美妆达人',
    '50016349': '下厨房',
    '50016348': '持家',
    '122852001': '持家',
    '21': '持家',
    '50008163': '持家',
    '122928002': '持家',
    '50025705': '持家',
    '122950001': '送礼',
    '122952001': '下厨房',
    '50020485': '理工男	动手能力强',
    '50008164': '有房',
    '124050001': '有房',
    '50020611': '行政',
    '50020332': '有房',
    '124568010': '有房',
    '50020808': '品质	生活',
    '27': '有房',
    '50020857': '品质	手工爱好者',
    '1625': '需细分',
    '16': '需细分',
    '50006843': '需细分',
    '50010404': '需细分',
    '50011740': '需细分',
    '30': '需细分',
    '3009': '需细分',
    '35': '细分',
    '50022517': '待产',
    '50014812': '婴儿',
    '25': '幼儿',
    '50008165': '有孩儿	送礼',
    '122650005': '有孩儿	送礼',
    '124468001': '三农',
    '124466001': '三农',
    '124470001': '三农',
    '50016891': '游戏达人',
    '50011665': '游戏达人',
    '99': '游戏达人',
    '40': '企鹅粉',
    '28': '品质男',
    '23': '收藏家',
    '124484008': '二次元',
    '50468001': '高品质',
    '50008171': '高品质',
    '50011397': '高品质',
    '1705': '爱美',
    '50013864': '爱美',
    '50026523': '生活',
    '50050471': '结婚',
    '29': '萌宠',
    '50025707': '旅游',
    '2813': '嘿嘿嘿',
    '50014927': '活到老学到老',
    '50454031': '旅游',
    '50025111': 'O2O',
    '50011949': '旅游	嘿嘿嘿',
    '50025110': '享受生活',
    '50026555': '持家',
    '50019095': '持家',
    '50008075': '吃货',
    '50007216': '品质生活',
    '50002768': '养生	爱美',
    '50010788': '爱美',
    '50023282': '爱美',
    '1801': '爱美',
    '50074001': '机车党',
    '26': '有车',
    '50013886': '户外',
    '124354002': '电驴一族',
    '122684003': '户外',
    '50010728': '健身	运动',
    '50510002': '户外',
    '50011699': '运动',
    '50010388': '运动',
    '50012029': '运动',
    '50020275': '养生	保健',
    '50008825': '养生	保健',
    '50026800': '养生	保健',
    '50026316': '小资',
    '50050359': '品质生活',
    '50012472': '持家',
    '50016422': '持家',
    '124458005': '爱茶',
    '50008141': '爱酒',
    '50002766': '吃货'
}
