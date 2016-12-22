#coding:utf-8
import re
import sys

for line in sys.stdin:
    fields = line.strip().split('\t')
    title=fields[-1]
    if type(title) == type(u""):
        title.encode("utf-8")
    else:
        title
    keywords_province= '北京|天津|上海|重庆|河北|河南|云南|辽宁|黑龙江|湖南|安徽|山东|新疆|江苏|浙江|江西|湖北|广西|甘肃|山西|内蒙古|陕西|吉林|福建|贵州|广东|青海|西藏|四川|宁夏|海南|台湾|香港|澳门'
    return_list = list(set(re.findall(keywords_province, title)))
    score = "%d"%len(return_list )
    province = '|'.join(return_list )
    fields.extend([score, province ])
    print "\t".join(fields)