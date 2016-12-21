#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')



'''
使用步骤：
1、先编写UDF文件，存为udfname.py
2、上传到HDFS
3、在HIVE上执行：
ADD FILE ADD FILE hdfs://10.3.4.220:9600/user/zlj/udf/udfname.py ;
注意每次修改udf都要重新加载udf文件覆盖原有文件。







select
TRANSFORM(item_id, feed_id, content)
USING 'python word_cut.py'
as (item_id, feed_id, content,content_cut)
from
(
select item_id, feed_id, content
from t_zlj_feed_tmp   limit
10
)t ;

#coding:utf-8
import re
import sys
import jieba

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
        return res
    else: return res


for info in sys.stdin:
    line = info.strip().split('\t')
    txt=line[-1]
    txt_cut=valid_jsontxt('_'.join(jieba.cut(txt)))
    line.append(txt_cut)
    print '\t'.join([valid_jsontxt(word) for word in line])

'''