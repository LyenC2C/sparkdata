#coding:utf-8
'''360
{"errno": 0, "data": {"q": "13765341804", "info": {"province": "贵州", "city": "安顺", "provider": "移动", "country": ""}, "labels": [{"labelNum": 6, "phoneNo": "13765341804", "label": "快递送餐"}, {"labelNum": 3, "phoneNo": "13765341804", "label": "响一声"}, {"labelNum": 1, "phoneNo": "13765341804", "label": "骚扰电话"}, {"labelNum": 1, "phoneNo": "13765341804", "label": "广告推销"}, {"labelNum": 1, "phoneNo": "13765341804", "label": "诈骗电话"}, {"labelNum": 1, "phoneNo": "13765341804", "label": "保险理财"}, {"labelNum": 1, "phoneNo": "13765341804", "label": "房产中介"}]}, "errmsg": ""}
'''

import json,sys,datetime

date = sys.argv[1]

def parse_360(line):
    j = json.loads(line.strip())
    if j.has_key("errno") and j["errno"] == 0:
        objls = []
        tel = j["data"]["q"]
        #print type(tel)
        if(j["data"].has_key("info") == False or j["data"]["info"] == None):
            return objls
        city = j["data"]["info"]["city"]
        province = j["data"]["info"]["province"]
        provider = j["data"]["info"]["provider"]
        lables = j["data"]["labels"]
        for lable in lables:
            #print lable["label"].encode("utf-8")
            today = str(datetime.date.today()).replace("-","")
            #l = lable["label"].encode("utf-8")+' '+str(lable["labelNum"])
            #print type(tel),type(province),type(city),type(provider),type(l)
            obj = {
                "tel" : tel,
                "province": province,
                "city": city,
                "provider" : provider,
                "tag" : lable["label"],
                "num" : lable["labelNum"],
                "platform":"360",
                "date":date
            }
            #print json.dumps(obj,ensure_ascii=False).encode("utf-8")
            #objls.append(obj)
            objls.append([obj["tel"],obj["province"],obj["city"],obj["provider"],obj["tag"],str(obj["num"]),obj["platform"],obj["date"]])
        return objls




for line in sys.stdin:
    tagls = parse_360(line)
    for each in tagls:
        #print json.dumps(each,ensure_ascii=False).encode("utf-8")
        print "\001".join(each).encode("utf-8")