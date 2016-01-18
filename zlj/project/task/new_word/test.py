#coding:utf-8
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
from __future__ import division
import argparse

"""
利用候选词序列词频文件，并计算每个候选词的右(左)邻字熵
由于此脚本只负责算词的邻熵，freq_file可以只包含两字及以上的词
"""
parser = argparse.ArgumentParser()
parser.add_argument("freq_file", help="candidate words file")
parser.add_argument("-s", "--separator", help="field separator", default="\t")
parser.add_argument("-f", "--freq_limit", help="word minimun frequence", default=1, type=int)
parser.add_argument("-r", "--reverse", help="when freq_file is reversed", action="store_true")
parser.add_argument("-o", "--output", help="Candidate Sequence Solidification File")

args = parser.parse_args()

src_file, des_file, freq_limit = args.freq_file, args.output, args.freq_limit

import re
re_chinese = re.compile(u'[^a-zA-Z0-9\u4e00-\u9fa5]+')

line='GPS 二进制存储文件是GPS 20110815.data，所有8月15日的GPS数据都可以在这个文件中查到。\001 \002 \t \r \n'
sentence = re_chinese.sub('', line.decode('utf-8').rstrip())

# for i in sentence:print i
print sentence