spark

conf = SparkConf()
conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
conf.set("spark.kryoserializer.buffer.mb","512")
conf.set("spark.broadcast.compress","true")
conf.set("spark.driver.maxResultSize","4g")
conf.set("spark.akka.timeout", "300")
conf.set("spark.shuffle.memoryFraction", "0.5")
conf.set("spark.core.connection.ack.wait.timeout", "1800")
conf.set("spark.hadoop.validateOutputSpecs", "false")
sc = SparkContext(appName="3_clean_fliter_corpus",conf=conf)