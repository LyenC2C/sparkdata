#coding:utf-8
from pyspark import SparkContext
#1)segment words using spark 2)count words using pyspark  3)load into hive table
'''
/*
segmentation:HanLP
shell-path:/home/hadoop/common/hanlp
spark-shell --driver-memory 6g --executor-memory 15g --executor-cores 5 --num-executors 15 --jars hanlp-1.2.9.jar
*/

val text = sc.textFile("/hive/warehouse/wl_service.db/t_lt_base_recommend_item_fraud_all/*").map{ x =>
      Predefine.HANLP_PROPERTIES_PATH = "/home/hadoop/common/hanlp/hanlp.properties"
      var sTemp=x.split("\001")
      val temp = HanLP.segment(sTemp(1))
      CoreStopWordDictionary.apply(temp)
      val word = for (i <- Range(0, temp.size())) yield temp.get(i).word
      //val Bi_words = for (i <- Range(0, word.length-1)) yield word(i).concat(word(i+1))
      //val all_words = word ++ Bi_words
      //sTemp(0)+"\t"+sTemp(1)+"\t"+all_words.mkString(" ")
      word.mkString(" ")
    }
text.saveAsTextFile("/user/lt/Hanlp_fenci")
'''

# 2) pyspark2 --driver-memory 6g --executor-memory 15g --executor-cores 5 --num-executors 15
sc = SparkContext(appName="testModel")
path = '/user/lt/Hanlp_fenci/*'
datas = sc.textFile(path).map(lambda x:x.split('\t'))
spam = datas.filter(lambda x:x[0]=='1').map(lambda x:x[-1])
normal = datas.filter(lambda x:x[0]=='0').map(lambda x:x[-1])

spam_wc=spam.flatMap(lambda x:x.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b)
spam_sort = spam_wc.sortBy(lambda x:x[1],ascending=False)
spam_sort.map(lambda (a,b):a+'\001'+str(b)).saveAsTextFile('/user/lt/spam_wc_sort')

normal_wc=normal.flatMap(lambda x:x.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b)
normal_sort = normal_wc.sortBy(lambda x:x[1],ascending=False)
normal_sort.map(lambda (a,b):a+'\001'+str(b)).saveAsTextFile('/user/lt/normal_wc_sort')


#3)load
"""
//hive table
CREATE TABLE  if not exists wl_analysis.t_lt_base_fraud_item_word_count (
word STRING  COMMENT  '词语',
word_count  STRING   COMMENT '词频'
)
COMMENT '电商异常商品词频表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  COLLECTION ITEMS TERMINATED BY ',';

load data INPATH '/user/lt/spam_wc_sort_0418/part*' into TABLE wl_analysis.t_lt_base_fraud_item_word_count;
"""