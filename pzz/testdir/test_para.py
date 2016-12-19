from pyspark import *



if __name__ == '__main__':
    conf = SparkConf()
    conf.set("spark.kryoserializer.buffer.mb","512")
    conf.set("spark.broadcast.compress","true")
    conf.set("spark.driver.maxResultSize","4g")
    conf.set("spark.akka.timeout", "300")
    conf.set("spark.shuffle.memoryFraction", "0.5")
    conf.set("spark.core.connection.ack.wait.timeout", "1800")
    conf.set("spark.default.parallelism","400")

    sc = SparkContext(appName="parallese test",conf=conf)
    rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/qun_member_info/ds=info/part-*")
    rdd.count()
    rdd1 = rdd.map(lambda x:x.split("\001")).map(lambda x:[x[0],x[5]]).groupByKey()
    rdd2 = rdd1.mapValues(list).map(lambda (x,y):y[0]).filter(lambda x:x!=None).map(lambda x:[x,1]).groupByKey()
    rdd2.count()
    sc.stop()