#coding:utf-8

import rapidjson as json
import sys

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")


def gen_item_feedid(line):
    ls = line.strip().split("\001")
    return [ls[0], ls[2]]


def parse_cmt_new(line_s):
    line = valid_jsontxt(line_s)
    ts = line[:line.find('\t')]
    # ts ='1445270400'
    # ts=str(time.mktime(datetime.datetime.now().timetuple()))
    json_txt = line.strip()[line.find('2(') + 2:-1]
    ob = json.loads(json_txt)
    print ob
    if type(ob) == type({}) and ob.has_key("data") and ob["data"].has_key("rateList"):
        data = ob['data']
        list = []
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
                int(userid)
                l.append(userid)

                # l.append(data.get('userStar'))
                feedback = value.get('feedback', '-').replace('\t', '')
                l.append(valid_jsontxt(feedback))
                date = value.get('feedbackDate', '-')
                l.append(date)
                l.append(value.get('annoy', '-'))
                l.append(ts)
                date = date.replace('-', '')
                int(date)
                if len(date) != 8:
                    print "date is wrong,now is "+date
                    continue
                # l.append(str(time.mktime(datetime.datetime.now().timetuple())))
                list.append(feedid)
            except Exception,e:
                print e,line
        return list
    return None

if __name__ == '__main__':
    for line in sys.stdin:
        ls = parse_cmt_new(line)
        if ls == None:
            print 'ls , none'
            continue
        for each in ls:
            print each
            if each != None:
                print each