#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="qun_member_process")

def is_chinese(uchar):
        """判断一个unicode是否是汉字"""
        if uchar >= u'\u4e00' and uchar<=u'\u9fa5':
                return True
        else:
                return False

def is_number(uchar):
        """判断一个unicode是否是数字"""
        if uchar >= u'\u0030' and uchar<=u'\u0039':
                return True
        else:
                return False

def is_alphabet(uchar):
        """判断一个unicode是否是英文字母"""
        if (uchar >= u'\u0041' and uchar<=u'\u005a') or (uchar >= u'\u0061' and uchar<=u'\u007a'):
                return True
        else:
                return False

def is_other(uchar):
        """判断是否非汉字，数字和英文字符"""
        if not (is_chinese(uchar) or is_number(uchar) or is_alphabet(uchar)):
                return True
        else:
                return False

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content

def f(line):
    result = []
    text = valid_jsontxt(line.replace("\\n", "").replace("\\r", "").replace("\\t", "").replace("\u0001", ""))
    ob = json.loads(valid_jsontxt(text))
    if type(ob) != type({}): return None
    qq_id = ob.get("qq_id","-")
    field1 = ob.get("field1","-")
    field2 = ob.get("field2","-")
    field3 = ob.get("field3","-")
    qun_id = ob.get("qun_id","-")
    name = ob.get("name","-").decode("utf-8")
    name_result = ""
    for ln in name:
        if not is_other(ln): name_result += ln
    result.append(valid_jsontxt(str(qq_id)))
    result.append(valid_jsontxt(str(field1)))
    result.append(valid_jsontxt(str(field2)))
    result.append(valid_jsontxt(str(field3)))
    result.append(valid_jsontxt(str(qun_id)))
    result.append(name_result)
    return '\001'.join(result)



s = "/data/develop/qq/group_member.json"
rdd = sc.textFile(s).map(lambda x:f(x)).filter(lambda x:x!=None)
rdd.saveAsTextFile('/user/wrt/qun_member_info')

#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 40 qun_member_process.py



