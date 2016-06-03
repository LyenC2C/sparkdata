#coding:utf-8
import pymongo

db = pymongo.Connection("192.168.4.200")["qq"]
coll_1 = db["group_member"]
coll_2 = db["qun_info"]
import sys

for line in sys.stdin:
    try:
        qqid = int(line.strip())
    except:
        continue
    for user in coll_1.find({"qq_id":qqid}):
        try:
            s = line.strip()+'\t'+user["name"].encode("utf-8")+'\t'+str(user["field2"])+'\t'+str(user["field1"])+'\t'
            quninfo = coll_2.find_one({"qun_id" :user["qun_id"]})
            if quninfo != None:
                #print type(quninfo['create_date']),type(quninfo["title"]),type(quninfo["qun_text"])
                s += quninfo['create_date'].encode("utf-8")+'\t'+str(user["qun_id"])+'\t'+quninfo["title"].replace("\t"," ").encode("utf-8")+'\t'+quninfo["qun_text"].replace("\r"," ").encode("utf-8")
                #s += str(user["qun_id"])+'\t'+quninfo["title"].replace("\t"," ").encode("utf-8")+'\t'+quninfo["qun_text"].replace("\r"," ").encode("utf-8")
            print s.strip()
        except:
            pass
