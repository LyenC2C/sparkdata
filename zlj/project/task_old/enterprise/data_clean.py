#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
import time
import rapidjson as json

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content
import itertools
def parse(line_s):
    try:
        rs=[]
        line=valid_jsontxt(line_s)
        ob=json.loads(line)
        if type(ob)==type(0.1):return None
        baseinfo=ob['Baseinfo']
        company=baseinfo.get('Company')
        no_id =int(company.get('No'))
        employees=company.get('Employees')
        for item in employees:
            name=item['Name']
            rs.append((name,no_id))
        return rs
    except: return None


def parse(line_s):
    try:
        rs=[]
        line=valid_jsontxt(line_s)
        ob=json.loads(line)
        if type(ob)==type(0.1):return None
        baseinfo=ob['Baseinfo']
        company=baseinfo.get('Company')
        no_id =company.get('No','-')
        if not no_id.isdigit():return None
        employees=company.get('Employees')
        companyName=company.get('Name','-')
        rs.append(companyName)
        rs.append(no_id)
        for item in employees:
            name=item.get('Name', '-')
            rs.append(name)
        return rs
    except: return None

def f(id_ids):
    lz=sorted([ i for i in list(set(id_ids)) if  i>0])
    return [i for i in itertools.combinations(lz, 2)]

# rdd=sc.parallelize(sc.textFile('/user/zlj/enterprise/Baseinfo.json').take(10))
# rdd.map(lambda x:parse(x)).take(1)
#
rdd=sc.textFile('/user/zlj/temp/v2.0.Baseinfo.change.json').map(lambda x:parse(x)).filter(lambda x:x is not None)

rdd.filter(lambda x:len(x)>2).count()
rdd.map(lambda x:'\t'.join([i for i in x if i is not None])).saveAsTextFile('/user/zlj/enterprise/Baseinfo.name.id.employees.all')

rdd.filter(lambda x:len(x)>2).map(lambda x:'\t'.join([i for i in x if i is not None])).saveAsTextFile('/user/zlj/enterprise/Baseinfo.name.id.employees.fiternoEmploy')


#
# rdd.flatMap(lambda x:x).groupByKey().map(lambda (x,id_ids):f(id_ids))

rdd1=rdd.flatMap(lambda x:x).groupByKey().map(lambda (x,id_ids):f(id_ids))

rdd2=rdd1.flatMap(lambda x:x).filter(lambda x:x is not None).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)

sc.textFile('/user/zlj/enterprise/Baseinfo.json').map(lambda x:parse(x)).flatMap(lambda x:x)\
    .groupByKey().map(lambda (x,id_ids):f(id_ids)).flatMap(lambda x:x).filter(lambda x:x is not None).map(lambda x:(x,1))\
    .reduceByKey(lambda a,b:a+b)

rdd2.saveAsTextFile('/user/zlj/enterprise/Baseinfo_ids')



sc.textFile('/user/zlj/enterprise/Baseinfo_ids/').map(lambda x:'\t'.join(x.replace('(','').replace(')','').split(','))).saveAsTextFile('/user/zlj/enterprise/Baseinfo_ids_info')






def parse_inv(line_s):
    try:
        rs=[]
        line=valid_jsontxt(line_s)
        ob=json.loads(line)
        if type(ob)==type(0.1):return None
        if 'search_info' not in ob:
            return None
        companyName=ob['search_info'].split()[-1]
        NO=ob['NO']
        rs.append(NO)
        Investment=ob['Investment']
        for i in  Investment:
            no=i.get('No','-')
            if no is None or len(no)<1:continue
            rs.append(no)
        return rs
    except:None

rdd=sc.textFile('/user/zlj/temp/v2.0.Invester.change.json').map(lambda x:parse_inv(x)).filter(lambda x:x is not None)

rdd.map(lambda x:'\t'.join(x)).saveAsTextFile('/user/zlj/enterprise/Investment.invid.invids')
# sc.textFile().flatMap(lambda x:x).reduceByKey().takeOrdered()
# ''.decode('utf-8')
rdd3=rdd.map(lambda x:(x[0].decode('utf-8'),[i.decode('utf-8') for i in x[1:]]))










rdd=sc.textFile('/user/zlj/enterprise/Investment.invid.invids').map(lambda x:x.split()).map(lambda x:[[x[0],i] for i in x[1:]]).flatMap(lambda x:x)\
    .map(lambda x:sorted(x,reverse=True)).map(lambda x:'\t'.join(x))

rdd.distinct().saveAsTextFile('/user/zlj/enterprise/Investment.invid.invid')
rddt=sc.textFile('/user/zlj/enterprise/Baseinfo.name.id.employees.fiternoEmploy').map(lambda x:[i.strip() for i in x.split('\t')]).filter(lambda x:len(x)>2)\
    .map(lambda x:[(i,x[1]) for i in set(x[2:])]).flatMap(lambda x:x)

# rdd.filter(lambda x:x[1]==u'440301103540681').collect()

rs=rddt.join(rddt)

rdd2=rs.map(lambda (x,y):sorted(y,reverse=True)).filter(lambda x: len(x)==2 and x[0]!=x[1])
# rdd2=rdd.join(rdd).(lambda (x,y):sorted([i  for i in set(list(y)) if len(set(i.split()))==2],reverse=True))


rdd2.map(lambda x:'\t'.join(x)).saveAsTextFile('/user/zlj/enterprise/Baseinfo.samename.id.id')


rdd2t=sc.textFile('/user/zlj/enterprise/Baseinfo.samename.id.id')

rdd3=rdd2t.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).filter(lambda x:x[1]>1)



rdd3=rdd2.map(lambda x:('\t'.join(x),1)).reduceByKey(lambda a,b:a+b).filter(lambda x:x[1]>1)

rdd3.map(lambda x:x[0]+'\t'+str(x[1])).saveAsTextFile('/user/zlj/enterprise/Baseinfo.samename.id.id.fiter_two')
rdd3.filter(lambda x:x[1]>2).map(lambda x:x[0]+'\t'+str(x[1])).saveAsTextFile('/user/zlj/enterprise/Baseinfo.samename.id.id.fiter_three')
# count 409536891
#       92518565


rdd3.filter(lambda x:x[1]>1).count()

rdd4=rdd2t.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).filter(lambda x:x[1]>1).map(lambda x:x[1].split()).map(lambda x:(x[0],x[1]))

rdd4.saveAsTextFile('/user/zlj/enterprise/Baseinfo.samename.id.id.fiter_two')
# 92504185


rdd.map(lambda x:(x,1))
rddts=sc.textFile('/user/zlj/enterprise/Baseinfo.samename.id.id.fiter_two').map(lambda x:x[1].split())

rdd4.map(lambda x:x[0]).subtract(rdd).count()

rdd3.map(lambda x:x[0]+'\t'+x[1])


# rdd.map(lambda x:x[0].decode('utf-8')+'\001'+'\t'.join([i.decode('utf-8') for i in x[1:]])).saveAsTextFile('/user/zlj/enterprise/Investment.comname.invids.table')
# rdd3.map(lambda x:(x[0].decode('utf-8'),1)).reduceByKey(lambda a,b:a+b)
#
#
# rdd2=rdd3.join(rdd1).map(lambda (x,y):[list(y)[1]+'\t'+i  for  i in  list(y)[0]])
#
#
# rdd2.map(lambda (x,y):[list(y)[1]+'\t'+i.decode('utf-8')  for  i in  list(y)[0]]).flatMap(lambda x:x).saveAsTextFile('/user/zlj/enterprise/Investment.id.invid')
#
#
# rdd2.filter(lambda x: '*'  in x[0]).take(10)
#
# rdd1=sc.textFile('/user/zlj/enterprise/Baseinfo.name.id.employees/').map(lambda x:x.split()).filter(lambda x: len(x)>2 and  u'*' not in x[0] and len(x[0])>4 and x[1].isdigit()).map(lambda x:x[0]+'\001'+x[1])
#
# sc.textFile('/user/zlj/enterprise/Baseinfo.name.id.employees/').map(lambda x:x.split())\
#     .filter(lambda x: len(x)>2 and  u'*' not in x[0] and len(x[0])>4 and x[1].isdigit()).map(lambda x:x[0]+'\001'+x[1])\
#     .saveAsTextFile('/user/zlj/enterprise/Baseinfo.name.id.employees.table')



#
# rdd=sc.textFile('/user/zlj/enterprise/Baseinfo.name.id.employees.table').map(lambda x:x.split('\001')).map(lambda x:(x[0],x[1]))
#
#
#
# rdd1=sc.textFile('/user/zlj/enterprise/Investment.comname.invids.table').map(lambda x:x.split('\001')).map(lambda x:(x[0],x[1].split('\t'))).filter(lambda x: u'*' not in x[0] and len(x[0])>4)
#
# rdd2=rdd.join(rdd1)
#
# ls=[]
