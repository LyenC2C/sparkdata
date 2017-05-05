
#统计用户记录表商品词语词频
from pyspark import SparkContext,SparkConf
from pyspark.sql import *

conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.kryoserializer.buffer.mb", "512")
conf.set("spark.broadcast.compress", "true")
conf.set("spark.driver.maxResultSize", "4g")
conf.set("spark.akka.timeout", "300")
conf.set("spark.shuffle.memoryFraction", "0.5")
conf.set("spark.core.connection.ack.wait.timeout", "1800")
conf.set("spark.hadoop.validateOutputSpecs", "false")
sc = SparkContext(appName="hmm", conf=conf)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
hiveContext.sql('use wl_analysis')

sql_hmm='''
select * from t_lt_base_ciyun_user_record_segment_words where ds='20170428'

'''
rdd_table = hiveContext.sql(sql_hmm).map(lambda x: x[1].split('_'))
#word count
words_count = rdd_table.flatMap(lambda x: x).map(lambda x: (x, 1)).reduceByKey(lambda a,b:a+b)
#sort
sorted_wc = words_count.sortBy(lambda x:x[1],ascending=False)
#filter frequence
sorted_wc.map(lambda (a,b):a+'\001'+str(b)).saveAsTextFile('/user/lt/ciyun/word_count')



#统计结果入库
CREATE TABLE  if not exists wl_analysis.t_lt_base_ciyun_record_word_count (
word STRING,
count  bigint
)
COMMENT '电商用户购买记录词频表'
PARTITIONED by (ds STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;


load data inpath '/user/lt/ciyun/word_count/*' into TABLE wl_analysis.t_lt_base_ciyun_record_word_count PARTITION (ds='20170505');