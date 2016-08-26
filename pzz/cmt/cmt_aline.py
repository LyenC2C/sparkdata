#coding:utf-8
import rapidjson as json
import sys

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\r", "")

#return [[[usermark,[feedid,"\001".join(l)]],[]],user_dic.keys]
def parse_cmt_v4(line_s):
    line = valid_jsontxt(line_s)
    ls = line.strip().split("\t")
    if len(ls) != 5:
        return None
    ts = ls[0]
    crawl_type=ls[2]
    json_txt = ls[4][11:-1]
    ob = json.loads(json_txt)
    itemid = ""
    if type(ob) == type({}) and ob.has_key("data") and ob["data"].has_key("rateList"):
        data = ob['data']
        feed_ls = []
        user_dic = {}
        for value in data['rateList']:
            try:
                l = []
                itemid = value.get('auctionNumId', '-')
                int(itemid)
                l.append(itemid)
                l.append(value.get('auctionTitle', '-').replace('\t', ''))
                feedid = value.get('id', '-')
                int(feedid)
                l.append(feedid)
                userid = value.get('userId', '-')
                usermark = value.get('userMark','')
                if len(usermark)!=24:
                    continue
                l.append(userid)
                # l.append(data.get('userStar'))
                feedback = value.get('feedback', '-').replace('\t', '')
                l.append(valid_jsontxt(feedback))
                date = value.get('feedbackDate', '-').replace(".","-")
                l.append(date)
                annoy = value.get('annoy', '-')
                l.append(annoy)
                l.append(ts)
                sku = json.dumps(value.get('skuMap'))
                l.append(sku)
                rate_type = value.get('rateType', '-')
                l.append(rate_type)
                l.append(crawl_type)
                l.append(usermark)
                #user
                user_nick = value.get('userNick', '-')
                user_star = value.get('userStar', '-')
                key = userid+'\001'+usermark+'\001'+user_nick+'\001'+user_star
                if user_dic.has_key(key):
                    pass
                else:
                    user_dic[key] = None
                #date
                date = date[:10].replace('-', '')
                int(date)
                if len(date) != 8:
                    print "date is wrong,now is "+date
                    continue
                # l.append(str(time.mktime(datetime.datetime.now().timetuple())))
                feed_ls.append([feedid, [usermark,"\001".join(l)]])
            except Exception,e:
                print e,line
        #return [[[feedid,[usermark,"\001".join(l)]],[]],user_dic.keys]
        return [feed_ls,None,itemid]
    else :
        #print 'not a json line',line_s.encode("utf-8")
        pass
    return None

if __name__ == '__main__':
    for line in sys.stdin:
        ls = parse_cmt_v4(line.strip())
        for each in ls[0]:
            print each[1][1]