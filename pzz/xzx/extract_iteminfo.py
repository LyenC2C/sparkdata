# coding:utf-8
import sys, rapidjson, time
import rapidjson as json

import zlib
import base64
import time

def compress(data):
    compressed = zlib.compress(data)
    out = base64.b64encode(compressed)
    return out


def decompress(out):
    decode = base64.b64decode(out)
    data = zlib.decompress(decode)
    return data

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res

def filter_line(line,id_dic):
    ls = line.strip().split("\001")
    if id_dic.has_key(ls[0]):
        return line.strip()
    return None

if __name__ == "__main__":
    if sys.argv[1] == "-h":
        print 'argvs:  [1]:-byitemid [2]:itemid inputpath, [3]outputpath'

    elif sys.argv[1] == '-byitemid':
        from pyspark import SparkContext
        sc = SparkContext(appName="xzx_iteminfo_extract")
        input_path = sys.argv[2]
        output_path = sys.argv[3]
        id_dic = sc.broadcast(sc.textFile(input_path).map(lambda x:(x.strip(),None)).collectAsMap())
        rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_ec_item_house")
        rdd.map(lambda x:filter_line(x,id_dic.value))\
            .filter(lambda x:x!=None)\
            .saveAsTextFile(output_path)
        sc.stop()
