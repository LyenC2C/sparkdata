# coding:utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import sys
from Queue import Queue
from pyspark import SparkContext

sc = SparkContext(appName="cleanWords")

def clean(line):
    q = Queue()
    items = line.split()
    words = []
    tmp = ''
    item_id = items[0].strip()
    for word in items[1:]:
        if '[|' in word:
            q.put(word)
        elif '|]' in word:
            q.get()
            if q.empty():
                words.append(tmp)
                tmp = ''
        else:
            if q.empty():
                words.append(word)
            else:
                tmp = tmp + word
    ls = [i for i in words if len(i)>1]
    line = "\001".join([item_id,'_'.join(ls)])
    return line

#test
data = sc.textFile("/user/lt/segments/test")
data.map(lambda x:clean(x)).saveAsTextFile("/user/lt/cleanWords2")

#run
data = sc.textFile("/user/lt/segments/cut*")
data.map(lambda x:clean(x)).saveAsTextFile("/user/lt/cleanWords")



"""
load data INPATH '/user/lt/cleanWords/part*' into TABLE wl_analysis.t_lt_base_item_segment_words PARTITION (ds='20170428');

drop table wl_analysis.t_lt_base_item_segment_words;
create table if not exists wl_analysis.t_lt_base_item_segment_words(
item_id bigint comment '商品id',
words string comment '商品分词结果'
)
comment '淘宝商品名分词结果表'
partitioned by (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' stored as textfile;

load data INPATH '/user/lt/cleanWords/part*' into TABLE wl_analysis.t_lt_base_item_segment_words PARTITION (ds='20170428');

(select item_id,user_id from t_lt_taobao_fraud_record_week where ds='20170425')t1
left join
(select )
"""