#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="qq_qun_process")

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
    create_date = ob.get("create_date","-")
    title = ob.get("title","-").decode("utf-8")
    title_result = ""
    for ln in title:
        if not is_other(ln): title_result += ln
    mast_qq = ob.get("mast_qq","-")
    qun_id = ob.get("qun_id","-")
    qun_class = ob.get("class","-")
    qun_text = ob.get("qun_text","-")
    qun_text_pro = qun_text.decode("utf-8")
    qun_text_result = ""
    for ln in qun_text_pro:
        if not is_other(ln): qun_text_result += ln
    # qun_text_result = valid_jsontxt(qun_text_result)
    result.append(valid_jsontxt(str(qun_id)))
    result.append(valid_jsontxt(str(qun_class)))
    result.append(valid_jsontxt(str(mast_qq)))
    result.append(valid_jsontxt(str(title_result)))
    result.append(valid_jsontxt(str(create_date)))
    result.append(valid_jsontxt(str(qun_text_result)))
    return '\001'.join(result)



s = "/data/develop/qq/qun_info.json"
rdd = sc.textFile(s).map(lambda x:f(x))
rdd.saveAsTextFile('/user/wrt/qq_qun_info')

#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 40 qq_qun_process.py



