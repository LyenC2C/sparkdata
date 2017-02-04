# encoding=utf-8
from pyspark import SparkContext
import Queue
'''
对分词后的关键字进行合并
'''

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

def recombine(strs):
    contentArray = strs.split()
    q = Queue.Queue()
    lv = []
    lv_2 = []
    tmp = ''
    for word in contentArray:
        if '[|' in word:
            q.put(word)
        elif '|]' in word:
            q.get()
            if q.empty():
                lv.append(tmp)
                if len(tmp) >= 2:
                    lv_2.append(tmp)
                tmp = ''
        else:
            if q.empty():
                lv.append(word)
                if len(word) >= 2:
                    lv_2.append(word)
            else:
                tmp = tmp + word
    return (' '.join(lv))+'\001' + ' '.join(lv_2)


sc = SparkContext(appName="recombine_words")
data = sc.textFile('/user/zlj/tmp/item_title_20161205/cut_part*')\
    .map(lambda a: ((a.split('\001', 2)[0]).strip(), (a.split('\001', 2)[1]).strip()))

fir = data.mapValues(lambda a: recombine(a))\
          .map(lambda a: a[0] + '\001' + a[1])\
          .saveAsTextFile('/user/lel/re')


