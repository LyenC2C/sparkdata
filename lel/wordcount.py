from pyspark import SparkContext


def redecode(str):
    return str.encode('utf-8')

sc = SparkContext("local[*]", "wordcount")
#"hdfs://master:9000/data/wordcount.txt"
text = sc.textFile("/user/lel/wordcount.txt")
words = text.flatMap(lambda x: x.split()).map(lambda x: (redecode(x), 1)).reduceByKey(lambda a, b: (a + b))
print words.map(lambda (a,b):(b,a)).top(10)


