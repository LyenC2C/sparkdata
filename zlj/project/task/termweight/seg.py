#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')


from pyspark import SparkContext

from pyspark.sql import *

sc = SparkContext(appName="term weight")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


class Stack:
     def __init__(self):
         self.items = []
     def isEmpty(self):
         return self.items == []
     def push(self, item):
         self.items.append(item)
     def pop(self):
         return self.items.pop()
     def peek(self):
         return self.items[len(self.items)-1]
     def size(self):
         return len(self.items)

def clean(line):
    q=Stack()
    ss=line.split()
    lv=[]
    tmp=''
    id =ss[0]
    for word in ss[1:]:
        if  '[|' in word:
            q.push(word)
        elif '|]' in word:
            if q.size()!=0:
                q.pop()
            if q.size()==0:
                lv.append(tmp)
                tmp=''
        else:
            if q.size()==0:
                lv.append(word)
            else:
                tmp=tmp+word
    return id+'\001'+'\t'.join([i for i in lv if len(i)>1 and  ( not i.replace('.','',).isdigit())])


from Queue import Queue
q=Queue()
l=['40750345677  包邮   卡通 绒布 [| 小 [| 零钱 包 |] |] 袋 [| 硬币 包 |] 韩国 可爱 女 [| 手 拿 包 |] [| 手机 包 |] [| 钥匙 包 |] TAIL']
for line in l:
    ss=line.split()
    lv=[]
    print line
    tmp=''
    for word in ss:

        if  '[|' in word:
            q.put(word)
        elif '|]':
            q.get()
            if q.empty():
                lv.append(tmp)
                tmp=''
        else:
            if q.empty():
                lv.append(word)
            else:
                tmp=tmp+word
    print ' '.join(lv)
    ''.isdigit()


# step1
sc.textFile('/user/zlj/project/termweight/title_corpus_seg_1206/').coalesce(100).map(lambda x:clean(x)).saveAsTextFile(
    '/user/zlj/project/termweight/title_corpus_aliseg_1206_clean'
)

# rdd=sc.parallelize(sc.textFile('/user/zlj/project/termweight/title_corpus_seg_1206/').take(1))
# count

# step2
ss=sc.textFile('/user/zlj/project/termweight/title_corpus_aliseg_1206_clean/')\
    .map(lambda x:x.split('\001')[1].split()).flatMap(lambda x:x).distinct()
lv=ss.collect()
ls=[]
for index,i in enumerate(list):
    ls.append(str(index)+'\001'+i)
sc.parallelize(ls).saveAsTextFile('/user/zlj/project/termweight/init_word_index')


#step3  index
rdd=sc.textFile('/user/zlj/project/termweight/init_word_index').map(lambda x:x.split('\001')).map(lambda x:(x[1],int(x[0])))
broadcastVar = sc.broadcast(rdd.collectAsMap())
wdic = broadcastVar.value

# change
sc.textFile('/user/zlj/project/termweight/title_corpus_aliseg_1206_clean/')\
    .map(lambda x:x.split('\001')).map(lambda x:x[0]+'\001'+' '.join([ str(wdic.get(i,'')) for i in x[1].split()])).saveAsTextFile(
'/user/zlj/project/termweight/title_corpus_aliseg_1206_clean_index'
)


# step 4
hiveContext.sql('use wlbase_dev')
hiveContext.sql('select item_id,user_id  from t_base_ec_item_feed_dev where ds >20150701 group by item_id,user_id')\
    .map(lambda x:x[0]+'\001'+x[1]).saveAsTextFile('/user/zlj/project/termweight/feed_0701')



# step5
# rdd1=sc.textFile('/user/zlj/project/termweight/feed_0701/').map(lambda x: x.split('\001')).map(lambda x:(int(x[0]),int(x[1])))
rdd1=sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_tmp/').map(lambda x: x.split('\001')).filter(lambda x: x[0].isdigit and x[1].isdigit()).map(lambda x:(int(x[0]),int(x[1])))
rdd2=sc.textFile('/user/zlj/project/termweight/title_corpus_aliseg_1206_clean_index/').map(lambda x:x.split('\001'))\
    .filter(lambda x:x[0].isdigit() and len(x[0])>1)\
    .map(lambda x:(int(x[0]),[int(i) for i in x[1].split() if i.isdigit() and len(i)>1]))
rdd=rdd1.join(rdd2)
def tcount(ls):
    lv=[]
    for i in ls:
        lv.extend(i)
    s=set(lv)
    re=[]
    for i in s:
        re.append(str(i)+"\002"+str(lv.count(i)))
    return '\003'.join(re)
rdd.map(lambda (x,y):(y[0],y[1])).groupByKey().map(lambda (x,y):x+"\001"+tcount(y)).saveAsTextFile('/user/zlj/project/termweight/joininfo')


# step6

sc.textFile('').map(lambda x:x.split('\001')[1].split()).flatMap(lambda x:(int(x),1)).reduceByKey()