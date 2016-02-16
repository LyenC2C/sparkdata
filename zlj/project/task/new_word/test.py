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


sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_feed2015_parse_v5/*').map(lambda x:x.split('\001'))


import argparse

"""
利用候选词序列词频文件，并计算每个候选词的右(左)邻字熵
由于此脚本只负责算词的邻熵，freq_file可以只包含两字及以上的词
"""
# parser = argparse.ArgumentParser()
# parser.add_argument("freq_file", help="candidate words file")
# parser.add_argument("-s", "--separator", help="field separator", default="\t")
# parser.add_argument("-f", "--freq_limit", help="word minimun frequence", default=1, type=int)
# parser.add_argument("-r", "--reverse", help="when freq_file is reversed", action="store_true")
# parser.add_argument("-o", "--output", help="Candidate Sequence Solidification File")
#
# args = parser.parse_args()
#
# src_file, des_file, freq_limit = args.freq_file, args.output, args.freq_limit

import re
re_chinese = re.compile(u'[^a-zA-Z0-9\u4e00-\u9fa5]+')

line='侄子 穿 上去 超级 帅 ～ ！ 嫂子 跟 妈妈 都 说 我 特别 会 买 ！ 嘎 嘎嘎 嘎嘎'
sentence = re_chinese.sub(' ', line.decode('utf-8').rstrip())

# for i in sentence:print i
print sentence,type(sentence)==type('')

line='商品:* 质量:嘎嘎  商品:嘎嘎 第一次:买  一个:吃  一个:掉 一个:送  里面:个 评价:晚 商品:哈哈 商品:不好意思 商品:呵呵 商品:买 商品:真是 商品:就是 商品:购买 商品:懒 商品:还是 商品:亲 以后:需要 商品:透 商品:嘿嘿 商品:热情 商品:耐心  ' \
          '商品:斤 商品:唉 差:多 商品:嘻嘻  商品:哈哈哈 效果:怎么样 亲:下手 不知道:是不是 质量:怎么样 商品:这样 时尚:大方 效果:如何  数:小 商品:怎么样 商品:錯  ' \
          ' 商品:抱歉 数:大 商品:温和 商品:忙 商品:想象 商品:个 商品:犹豫 天:冷 商品:想 不知道:起 好评:好 品:那种 商品:说实话 颜色:没 不知道:用  商品:用  商品:极 ' \
          '效果:怎样 商品:伤心 商品:郁闷 商品:累 商品:好贴 质量:还是 商品:仙 里面:还有 亲:犹豫 棒:极 上:好看 商品:件  天气:冷 体重:斤  亲:放心 我:用  商品:滑滑 ' \
          '质量:如何 不知道:是 回头率:高' \
          '商品:不知道 商品:高兴 商品:润 商品:重要 商品:闪 小:多 商品:年 商品:啦啦啦 商品:元 商品:穿 商品:那种 我:在 我:给 商品:有 感:覺 己:经 之前:买 商品:送 亲:下' \
          '商品:财源广进 我:给 亲:买 商品:好孩子 孩子:岁 第一次:是 商品:家 玩:开心 性:强 不知道:长'

for ls in line.split():print ls


import os
def fun(line):
    lv=line.strip().split()
    path=lv[0]
    tmps=path.split('/')
    filename=tmps[-1]
    mvpath='/mnt/hdfs/data1/hdfsbck/record/record/'
    blks=lv[1:]
    size=len(blks)
    for index,blk in enumerate(blks):
        if os.path.isfile(blk):
            if  index <(size-1) and not os.path.isfile(blks[index+1]):
                os.popen('sed -i $d '+blk)
            if index>0 and  not os.path.isfile(blks[index-1]):
                os.popen('sed -i  1d '+blk)
            os.popen('cp '+blk +' ../tmp/')
    os.popen('cat ../tmp/* >../tmp/'+filename)
    os.popen('mv ../tmp/'+filename+" "+mvpath)
    os.popen('rm ../tmp/*')

for line in open('./fs_0201_dirs_tree_name_blk.file.csv.record'):
    fun(line)




import os
def fun(line):
    lv=line.strip().split()
    path=lv[0]
    tmps=path.split('/')
    filename=tmps[-1]
    mvpath='/mnt/hdfs/data1/hdfsbck/t_zlj_feed2015_parse_v5/t_zlj_feed2015_parse_v5'
    blks=lv[1:]
    size=len(blks)
    for index,blk in enumerate(blks):
        if os.path.isfile(blk):
            if  index <(size-1) and not os.path.isfile(blks[index+1]):
                os.popen('sed -i $d '+blk)
            if index>0 and  not os.path.isfile(blks[index-1]):
                os.popen('sed -i  1d '+blk)
            os.popen('cp '+blk +' ../tmp1/')
    os.popen('cat ../tmp1/* >../tmp1/'+filename)
    os.popen('mv ../tmp1/'+filename+" "+mvpath)
    os.popen('rm ../tmp1/*')

def fun1(line):
    lv=line.strip().split()
    path=lv[0]
    tmps=path.split('/')
    filename='/'.join(tmps[-2:])
    mvpath='/mnt/hdfs/data1/hdfsbck/itemsale/itemsale/'
    blks=lv[1:]
    size=len(blks)
    os.popen('cat '+' '.join(blks)+' >'+mvpath+filename)

for line in open('./fs_0201_dirs_tree_name_blk.file.csv.t_zlj_feed2015_parse_v5'):
    fun(line)
# line='/hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev/ds=20150629/20151231-0000	1075832949	1075832950	1075832951	1075832952	1075832953	1075832954 1075832955	1075832956	1075832957'




# /hive/warehouse/wlbase_dev.db/t_base_ec_item_sale_dev/ds=20160117/part-00050    1076430135      1076430268