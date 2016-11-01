#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

# from pyspark import SparkContext
# from pyspark.sql import *
# from pyspark.sql.types import *
# import time
# import rapidjson as json
#
# import rapidjson as json
def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content

# sc = SparkContext(appName="cmt")
# sqlContext = SQLContext(sc)
# hiveContext = HiveContext(sc)
# def f(x):
#     va=json.loads(valid_jsontxt(x))
#     ls=[]
#     ls.append(va['item_info'].get('title',''))
#     # ls.append(va['item_info'].get('item_id',''))
#     # ls.append(va['item_info'].get('brand_model',''))
#     # ls.append(va['item_info'].get('brand',''))
#     # ls.append(va['item_info'].get('category_id',''))
#     return ' '.join([valid_jsontxt(i) for i in ls])
#     # return ls
#
# sc.textFile('/user/zlj/aipusheng.json').map(lambda x:f(x)).repartition(1).saveAsTextFile('/user/zlj/aipusheng_info_title')
#


aipusheng_maps={
'E9503279-D80E-5C26-63B2-0649AA08A18F':'ELPLP41',
'DAEB1D6B-2240-A438-81ED-259C5BE5AFFF':'T672',
'56B2F1EE-93AE-99AF-B4FB-347796152196':'ELPLP50',
'E8BE82D4-461D-39A6-5E72-3D8FE8888372':'T664',
'3E15910A-7AB7-0F9D-8D16-3D9B209EFE19':'T141',
'FC29549F-BFCC-BCEC-5323-3F297DC16BB2':'T109',
'2A2C20C8-6020-CF59-8C4A-4B9F1DADF8D8':'T673',
'283E7DDF-4DC0-03EF-3B13-4F5C05EBDDB6':'ELPLP57',
'0597199E-E9FE-4DA8-A88E-57680F5AEC91':'T112',
'A21FF054-77EE-5C6C-14BD-57AAEC0104F9':'T071',
'4EDB50C4-96FC-EF3E-0536-7F4E8FD390CF':'T137',
'BA1384C4-FCBA-E2D4-4B22-828A599F610B':'T128',
'1A6CEECA-41E3-C311-0E0C-A7D526875EDC':'T119',
'66EA5C90-25D8-B0BA-E44B-AB8BA62A1F4E':'ELPLP42',
'0B7E6AE8-5E8F-D685-3268-AFE223EDA139':'T674',
'94ABBDB8-F252-2BA3-D433-C4C0BCD62EF0':'T089',
'0FF8C7EB-8AD6-D683-10EE-D160211E538F':'T129',
'931D8BDA-035A-D138-2190-FE2BCA0C77E2':'T180'

}
jianeng_maps={
    '8BC4FC1E-47F0-48E7-9E3F-0B03B16E61EA':'PG-40',
'4973B392-4E26-4A7B-4350-17FB4CD056B7':'CLI-8CMY',
'CE66FF5B-7CC3-33F0-60CE-21CDD0016E07':'PG-840',
'B38796CE-ACAA-52CA-16AA-40D6C2CB6139':'CL-816',
'B54F2B8F-FD53-4BAF-A97C-5AF35CA5EC96':'PG-815',
'6410E571-374A-5FFF-8B45-6280F6528027':'PG-830',
'8A97808F-DC4E-F175-2890-6A88A4080B22':'CL-831',
'793433CB-CED6-2507-3FB6-9CC2FA6A5BB7':'CL-41',
'BCBF7388-8489-2CB3-37C7-BB5E53ED2B84':'CL-841',
'4F765F42-E719-AE97-8738-C834113C442D':'CLI-826CMY'
}

fw=open("E:\\work\\xinzhengxin\\jianengmohe_info_title.txt_find",'w')
for line in open("E:\\work\\xinzhengxin\\jianengmohe_info_title.txt"):
    lv=[]
    for k,v in jianeng_maps.items():

        if v in line :lv.append(k+"^"+v)
    if len(lv)==0:continue
    fw.write(''.join(line.split()).strip().decode('utf-8') +'\t'+'\t'.join(lv)+"\n")
#
#
# fw=open("E:\\work\\xinzhengxin\\aipusheng_info_title.txt_find",'w')
# for line in open("E:\\work\\xinzhengxin\\aipusheng_info_title.txt"):
#     line=''.join(line.split()).strip()
#     lv=[]
#     for k,v in aipusheng_maps.items():
#         if v in line :
#             lv.append(k+"^"+v)
#     if len(lv)==0:continue
#     fw.write(line.decode('utf-8') +'\t'+'\t'.join(lv)+"\n")



class arithmetic():

    def __init__(self):
        pass
    ''''' 【编辑距离算法】 【levenshtein distance】 【字符串相似度算法】 '''
    def levenshtein(self,first,second):
        if len(first) > len(second):
            first,second = second,first
        if len(first) == 0:
            return len(second)
        if len(second) == 0:
            return len(first)
        first_length = len(first) + 1
        second_length = len(second) + 1
        distance_matrix = [range(second_length) for x in range(first_length)]
        #print distance_matrix
        for i in range(1,first_length):
            for j in range(1,second_length):
                deletion = distance_matrix[i-1][j] + 1
                insertion = distance_matrix[i][j-1] + 1
                substitution = distance_matrix[i-1][j-1]
                if first[i-1] != second[j-1]:
                    substitution += 1
                distance_matrix[i][j] = min(insertion,deletion,substitution)
        # print distance_matrix
        return distance_matrix[first_length-1][second_length-1]
map_oulaiya={}
for   line in open("E:\\work\\xinzhengxin\\oulaiya_ xinghao_1_dealclean_join_kvs.txt"):
    ls=line.split()
    if((len(ls))<3):continue
    k=ls[0].strip()
    v=ls[1].strip()
    b=ls[2].strip()
    map_oulaiya[k]=(v,b)


import Levenshtein

# fw=open("E:\\work\\xinzhengxin\\oulaiya_info_title.txt_find_v2",'w')
# for   index,line in enumerate(open("E:\\work\\xinzhengxin\\oulaiya_info_title.txt")):
#     line=line.strip()
#     lv=[]
#     arith = arithmetic()
#     asim=0
#     for k,v in map_oulaiya.items():
#         if v[1] in line :
#             sim=arith.levenshtein(v[0],line)
#
#             lv.append([k+"^"+v[0],sim])
#
#     # print line.decode('utf-8'),k+"^"+' '.join(v).decode('utf-8')
#     # print '---------------------'
#     if(len(lv)==0):continue
#
#     # ls=[]
#
#     lk=sorted(lv,key=lambda t:float(t[-1]),reverse=False)
#     asim=sum([1.0/(i[-1]+1) for index,i in enumerate(lk) if  index<3])
#     for index,i in enumerate(lk):
#         lk[index][1]=str(round((1.0/(lk[index][1]+1))/asim,2))
#     # for index,i in enumerate(lk):print index,i
#     # print ' '.join([' '.join(i)  for index,i in enumerate(lv) if index<3])
#     # print lv
#     # '\t'.join(['^'.join(i)  for index,i in enumerate(lk) if index<3])
#     # ''.join(line.split()).strip().decode('utf-8')
#     fw.write(''.join(line.split()).strip().decode('utf-8') +'\t'+'\t'.join(['^'.join(i)  for index,i in enumerate(lk) if index<3])+"\n")